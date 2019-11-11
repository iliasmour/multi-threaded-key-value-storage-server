/* server.c

   Sample code of 
   Assignment L1: Simple multi-threaded key-value server
   for the course MYY601 Operating Systems, University of Ioannina 

   (c) S. Anastasiadis, G. Kappes 2016

*/
//Ntoymanopoulos Xristos 2509
//Hlias Mourtos 2302

#include <signal.h>
#include <sys/stat.h>
#include "utils.h"
#include "kissdb.h"
#include <sys/time.h>
#include <stdio.h>
#include <pthread.h>


#define MY_PORT                 6767
#define BUF_SIZE                1160
#define KEY_SIZE                 128
#define HASH_SIZE               1024
#define VALUE_SIZE              1024
#define MAX_PENDING_CONNECTIONS   10

#define MAXSIZE 6
#define NUMTHREAD 4

pthread_mutex_t queue = PTHREAD_MUTEX_INITIALIZER; //otan peirazo oura
pthread_mutex_t time1 = PTHREAD_MUTEX_INITIALIZER; //otan peirazo xrono
pthread_mutex_t sigxronismos = PTHREAD_MUTEX_INITIALIZER; //ston sigxronismo anagnoston grafeon
pthread_cond_t filling_queue = PTHREAD_COND_INITIALIZER; 
pthread_cond_t empting_queue = PTHREAD_COND_INITIALIZER; 
pthread_cond_t s = PTHREAD_COND_INITIALIZER; //ston sigxronismo
int completed_requests=0; 
int writers=0;
int readers=0;
struct timeval total_waiting_time;
struct timeval total_service_time;

typedef struct queuerequest {
	int new_fd;
	struct timeval starting_time;
} queueRequest;

// Definition of the queue.
typedef struct queue {
	
	int rear;
	queueRequest requests[MAXSIZE];
} Queue;

Queue q1;
// Definition of the operation type.
typedef enum operation {
  PUT,
  GET
} Operation; 

// Definition of the request.
typedef struct request {
  Operation operation;
  char key[KEY_SIZE];  
  char value[VALUE_SIZE];
} Request;


// Definition of the database.
KISSDB *db = NULL;

/**
 * @name parse_request - Parses a received message and generates a new request.
 * @param buffer: A pointer to the received message.
 *
 * @return Initialized request on Success. NULL on Error.
 */
Request *parse_request(char *buffer) {
  char *token = NULL;
  Request *req = NULL;
  
  // Check arguments.
  if (!buffer)
    return NULL;
  
  // Prepare the request.
  req = (Request *) malloc(sizeof(Request));
  memset(req->key, 0, KEY_SIZE);
  memset(req->value, 0, VALUE_SIZE);

  // Extract the operation type.
  token = strtok(buffer, ":");    
  if (!strcmp(token, "PUT")) {
    req->operation = PUT;
  } else if (!strcmp(token, "GET")) {
    req->operation = GET;
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the key.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->key, token, KEY_SIZE);
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the value.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->value, token, VALUE_SIZE);
  } else if (req->operation == PUT) {
    free(req);
    return NULL;
  }
  return req;
}

/*
 * @name process_request - Process a client request.
 * @param socket_fd: The accept descriptor.
 *
 * @return
 */

void CtrlZ(){ //epeidi apla diavazo apo koinoxristes metavlites dn xrisimopoio locks


	struct timeval average_waiting_time;
	struct timeval average_service_time;

	long waiting_time=(total_waiting_time.tv_sec*1000000+total_waiting_time.tv_usec)/completed_requests; //long giati megala noumera,ta metatrepo ola se usec,kano tin praksi kai ta ksanakano sec
	long service_time=(total_service_time.tv_sec*1000000+total_service_time.tv_usec)/completed_requests;
	average_waiting_time.tv_sec=waiting_time/completed_requests;
	average_waiting_time.tv_usec=waiting_time%completed_requests;
	average_service_time.tv_sec=service_time/completed_requests;
	average_service_time.tv_usec=service_time%completed_requests;
	
	printf("Completed requests: %d\nAverage waiting time: %ld  sec and %ld  usec \nAverage service time: %ld  sec and %ld  usec\n",completed_requests,average_waiting_time.tv_sec,average_waiting_time.tv_usec,average_service_time.tv_sec,average_service_time.tv_usec);
	exit(0);
}



int isQueueEmpty(){
	
	if(q1.rear==-1){
		return 1;
	}
	return 0;
}

int isQueueFull(){

	if(q1.rear==MAXSIZE-1){
		return 1;
	}
	return 0;
}

void insert(queueRequest r1) 
{

	pthread_mutex_lock(&queue); //giati peirazo tin oura
	while(isQueueFull(q1)==1){
		pthread_cond_wait(&filling_queue,&queue); //otan einai gemati perimeno
	}
	
	q1.rear = q1.rear + 1;

	q1.requests[q1.rear]=r1;

	pthread_cond_signal(&empting_queue);
	pthread_mutex_unlock(&queue);

} /*End of insert()*/

queueRequest removeQ()
{
	queueRequest r1;

	pthread_mutex_lock(&queue); //peirazo tin oura
	while(isQueueEmpty(q1)==1){ //otan einai adeia perimena
		pthread_cond_wait(&empting_queue,&queue);
	}

	r1=q1.requests[0];
	for(int i=0;i<q1.rear;i++){
		q1.requests[i]=q1.requests[i+1];
	}
	q1.rear = q1.rear - 1;
	pthread_cond_signal(&filling_queue);

	pthread_mutex_unlock(&queue);
	
	return r1;

} /*End of insert()*/


void process_request(const int socket_fd) {
  char response_str[BUF_SIZE], request_str[BUF_SIZE];
    int numbytes = 0;
    Request *request = NULL;

    // Clean buffers.
    memset(response_str, 0, BUF_SIZE);
    memset(request_str, 0, BUF_SIZE);
    
    // receive message.
    numbytes = read_str_from_socket(socket_fd, request_str, BUF_SIZE);
    
    // parse the request.
    if (numbytes) {
      request = parse_request(request_str);
      if (request) {
        switch (request->operation) {
          case GET:

            pthread_mutex_lock(&sigxronismos); //sigxronismos grafeis-anagnostes
              while(writers==1){
                pthread_cond_wait(&s,&sigxronismos);
              }
              readers=readers+1;
            pthread_mutex_unlock(&sigxronismos);

            // Read the given key from the database.
            if (KISSDB_get(db, request->key, request->value))
              sprintf(response_str, "GET ERROR\n");
            else
              sprintf(response_str, "GET OK: %s\n", request->value);

            pthread_mutex_lock(&sigxronismos); //meiosi readers epeidi teleiose
              readers=readers-1;
            pthread_mutex_unlock(&sigxronismos);

            break;
          case PUT:

            pthread_mutex_lock(&sigxronismos); //sigxronismos grafeis-anagnostes
              while(writers==1||readers>0){
                pthread_cond_wait(&s,&sigxronismos);
              }
            pthread_mutex_unlock(&sigxronismos);

            // Write the given key/value pair to the database.
            if (KISSDB_put(db, request->key, request->value)) 
              sprintf(response_str, "PUT ERROR\n");
            else
              sprintf(response_str, "PUT OK\n");

            pthread_mutex_lock(&sigxronismos); //meiosi writers epeidi teleiose
              writers=writers-1;
            pthread_mutex_unlock(&sigxronismos);

            break;
          default:
            // Unsupported operation.
            sprintf(response_str, "UNKOWN OPERATION\n");
        }
        // Reply to the client.
        write_str_to_socket(socket_fd, response_str, strlen(response_str));
        if (request)
          free(request);
        request = NULL;
        return;
      }
    }
    // Send an Error reply to the client.
    sprintf(response_str, "FORMAT ERROR\n");
    write_str_to_socket(socket_fd, response_str, strlen(response_str));
}


void *threadsFunc(){
	queueRequest r1;
	struct timeval end_time,middle_time;
	float waiting_sec,waiting_usec,service_sec,service_usec;
	while(1){
		r1=removeQ();
		gettimeofday(&middle_time, NULL);

		process_request(r1.new_fd);
		close(r1.new_fd);
		gettimeofday(&end_time, NULL);

		waiting_sec=middle_time.tv_sec-r1.starting_time.tv_sec;
		waiting_usec=middle_time.tv_usec-r1.starting_time.tv_usec;
		if (waiting_usec>1000000){
			waiting_sec++;
			waiting_usec=waiting_usec-1000000;
		}
		if (waiting_usec<0){
			waiting_sec--;
			waiting_usec=waiting_usec+1000000;
		}
		service_sec=end_time.tv_sec-middle_time.tv_sec;
		service_usec=end_time.tv_usec-middle_time.tv_usec;
		if (service_usec>1000000){
			service_sec++;
			service_usec=service_usec-1000000;
		}
		if (service_usec<0){
			service_sec--;
			service_usec=service_usec+1000000;
		}
	
		//enimerosi xronon me locks
		pthread_mutex_lock(&time1);

			total_waiting_time.tv_sec=total_waiting_time.tv_sec+waiting_sec;
			total_waiting_time.tv_usec=total_waiting_time.tv_usec+waiting_usec;

			if(total_waiting_time.tv_usec>1000000){ //metatropi apo usec se sec
				total_waiting_time.tv_sec=total_waiting_time.tv_sec+1;
				total_waiting_time.tv_usec=total_waiting_time.tv_usec-1000000;
			}

			total_service_time.tv_sec=total_service_time.tv_sec+service_sec;
			total_service_time.tv_usec=total_service_time.tv_usec+service_usec;

			if(total_service_time.tv_usec>1000000){ //metatropi apo usec se sec
				total_service_time.tv_sec=total_service_time.tv_sec+1;
				total_service_time.tv_usec=total_service_time.tv_usec-1000000;
			}
		
			completed_requests=completed_requests+1;

		pthread_mutex_unlock(&time1);

	}

}

void initializeThreads(){
	pthread_t nimata[NUMTHREAD];
	int i;
	for(i=0;i<NUMTHREAD;i++){
		pthread_create(&nimata[i],NULL, threadsFunc, NULL);
	}

	//arxikopoiisi xronon
	total_waiting_time.tv_sec=0;
	total_waiting_time.tv_usec=0;
	total_service_time.tv_sec=0;
	total_service_time.tv_usec=0;

}

/*
 * @name main - The main routine.
 *
 * @return 0 on success, 1 on error.
 */
int main() {

  int socket_fd,              // listen on this socket for new connections
      new_fd;                 // use this socket to service a new connection
  socklen_t clen;
  
  queueRequest k1;
 

  struct sockaddr_in server_addr,  // my address information
                     client_addr;  // connector's address information

  q1.rear=-1;
  initializeThreads();

  // create socket
  if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    ERROR("socket()");

  // Ignore the SIGPIPE signal in order to not crash when a
  // client closes the connection unexpectedly.
  signal(SIGPIPE, SIG_IGN);


  signal(SIGTSTP, CtrlZ); //Control Z
  
  // create socket adress of server (type, IP-adress and port number)
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);    // any local interface
  server_addr.sin_port = htons(MY_PORT);
  
  // bind socket to address
  if (bind(socket_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) == -1)
    ERROR("bind()");
  
  // start listening to socket for incomming connections
  listen(socket_fd, MAX_PENDING_CONNECTIONS);
  fprintf(stderr, "(Info) main: Listening for new connections on port %d ...\n", MY_PORT);
  clen = sizeof(client_addr);

  // Allocate memory for the database.
  if (!(db = (KISSDB *)malloc(sizeof(KISSDB)))) {
    fprintf(stderr, "(Error) main: Cannot allocate memory for the database.\n");
    return 1;
  }
  
  // Open the database.
  if (KISSDB_open(db, "mydb.db", KISSDB_OPEN_MODE_RWCREAT, HASH_SIZE, KEY_SIZE, VALUE_SIZE)) {
    fprintf(stderr, "(Error) main: Cannot open the database.\n");
    return 1;
  }
  // main loop: wait for new connection/requests
  while (1) { 
    // wait for incomming connection
    if ((new_fd = accept(socket_fd, (struct sockaddr *)&client_addr, &clen)) == -1) {
      ERROR("accept()");
    }
    
    // got connection, serve request
    fprintf(stderr, "(Info) main: Got connection from '%s'\n", inet_ntoa(client_addr.sin_addr));

    k1.new_fd=new_fd;
    gettimeofday(&k1.starting_time, NULL);
    insert(k1);

  }  

  // Destroy the database.
  // Close the database.
  KISSDB_close(db);

  // Free memory.
  if (db)
    free(db);
  db = NULL;

  return 0; 
}

