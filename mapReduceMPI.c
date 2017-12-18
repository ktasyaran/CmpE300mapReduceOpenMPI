#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mpi.h"

//Arbitrarily string length is defined here.
#define MAX_LENGTH 255
/* Required shell command to count # of words in a file. 
Space is included to add more elements to given UNIX command.
*/
#define WORD_COUNT_COMMAND "wc -l "
//Output filename.

//For defining the master processor. Only it will split the data to the slaves.
#define MASTER_PROCESSOR 0

/*
  Word structure for mapping and reducing them. (Provides easier operation/calculation)
*/
typedef struct{
  int occurrence;
  char word[MAX_LENGTH];
}myWord;

int main(int argc,char**argv){
  //Processor ID and number of processors.
  int mypid;
  int numprocs;
  //File pointer to handle with I/O operations.
  FILE * fp;
  //Complete shell command to count words in a given file.
  char command[MAX_LENGTH];
  //Buffer array for getting elements.
  char buffer[MAX_LENGTH];
  unsigned int strLen;
  //Iteration count for getting tokens from file.
  unsigned int iterCount=0;
  //Temporary buffer array to eliminate \n character from given string.
  char* newBuffer;
  //To get current user's home directory.
  char* homedir;
  //Divider and remainder for array size.
  int divider;
  int remainder;
  //Temporary array size for splitting array to the slave processors.
  int tempSize;
//Necessary initializations.
  char ** rtem;
  int count;
  char ** corpus;
  int startValue=0;
  int i,j,k;
  int used=0;
  char geth[255];

  myWord sender;
  myWord receiver;
  myWord* allWords;
  myWord* smallWords;
  myWord* reduced;
  //Block lengths and displacements for user defined struct.
  int blockLen[2]={1,MAX_LENGTH};
  MPI_Aint disp[2];
  MPI_Datatype cWord;

  MPI_Init(&argc,&argv);
  //Initialize MPI environment.
  MPI_Comm_rank(MPI_COMM_WORLD,&mypid);
  MPI_Comm_size(MPI_COMM_WORLD,&numprocs);

  MPI_Get_address(&(sender.occurrence),&(disp[0]));
  MPI_Get_address(&(sender.word),&(disp[1]));

  //Type declaration and adjust displacements.
  MPI_Datatype types[2]={MPI_INT,MPI_CHAR};
  disp[1]=disp[1]-disp[0];
  disp[0]=(MPI_Aint) 0;

  //Creates user defined struct.
  MPI_Type_create_struct(2,blockLen,disp,types,&cWord);
  //Commits this type.
  MPI_Type_commit(&cWord);



  //For distributing corresponding array sizes to slave processors.
 int sizes[numprocs];
  if(mypid==MASTER_PROCESSOR){
    if(argc!=3){
      puts("Usage: name of executor INPUT_FILE OUTPUT_FILE");
      exit(-1);
    }
  strcat(command,WORD_COUNT_COMMAND);
  strcat(command,argv[1]);
  //Read output from command.
  fp=popen(command,"r");
  //Get number of words in the source file.
  fscanf(fp,"%d",&count);
  //Close file pointer.
  fclose(fp);
  //All elements read from file will be stored here.
  corpus=malloc(sizeof(char*)*count);
  //Open file pointer.
  fopen(argv[1],"r");

/*
Elements are assigned from file to array elements with deleting \n characters also.
*/
  while(fgets(buffer,MAX_LENGTH,fp)){
   //Gets current string length to remove \n
   strLen=strlen(buffer);
   newBuffer=malloc(strLen*sizeof(char));
   //Copies main string without \n symbol.
   memcpy(newBuffer,buffer,strLen-1);
   //To indicate that string is ended with null character instead of newline.
   newBuffer[strLen-1]='\0';
   //Memory allocation for new string in the word list. (corpus in this case)
   corpus[iterCount]=malloc(sizeof(char)*strLen);
   //Copies new string (without \n) to main word list.
   strcpy(corpus[iterCount],newBuffer);
   iterCount++;
  }
  
  //Close file pointer.
  fclose(fp);
   divider=iterCount/(numprocs-1);
  remainder=iterCount%(numprocs-1);
  //Size assignment for slave processors.
    sizes[0]=divider;
  for(i=1;i<numprocs;i++){
    sizes[i]=(i==numprocs-1) ? (divider+remainder):divider;
  }
 
  
}
//Scatter expected array sizes to slaves.
MPI_Scatter(sizes,1,MPI_INT,&tempSize,1,MPI_INT,MASTER_PROCESSOR,MPI_COMM_WORLD);

//Size adjustments for both master and slave processors.
if(mypid!=MASTER_PROCESSOR){
rtem=malloc(tempSize*sizeof(char*));
smallWords=malloc(tempSize*sizeof(myWord));
}
else{
  reduced=malloc(count*sizeof(myWord));
  allWords=malloc(count*sizeof(myWord));
}

 if(mypid==MASTER_PROCESSOR){
      //Loop for splitting array to processors.
      for(i=1;i<numprocs;i++){ 
        int placeHolder=0;
        for(j=0;j<sizes[i];j++){
             MPI_Send(corpus[(i-1)*sizes[i-1]+j],MAX_LENGTH,MPI_CHAR,i,1,MPI_COMM_WORLD);
             placeHolder++;
         }    
        }
  }  
   
   else{
    for(i=0;i<tempSize;i++){
      //Data from master processor is received.
      rtem[i]=malloc(MAX_LENGTH);
      MPI_Recv(rtem[i],MAX_LENGTH,MPI_CHAR,MASTER_PROCESSOR,MPI_ANY_TAG,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    }
    //Strings are mapped and returned to master again.
    for(i=0;i<tempSize;i++){ 
    receiver.occurrence=1;
    strcpy(receiver.word,rtem[i]);
    MPI_Send(&receiver,1,cWord,MASTER_PROCESSOR,1,MPI_COMM_WORLD);
  }
   }
/*
  Receive input from slaves and split mapped input to slaves again to be sorted.
*/
if(mypid==MASTER_PROCESSOR){
  for(i=1;i<numprocs;i++){
        for(j=0;j<sizes[i];j++){
        MPI_Recv(&allWords[(i-1)*sizes[i-1]+j],1,cWord,i,MPI_ANY_TAG,MPI_COMM_WORLD,MPI_STATUS_IGNORE);  
         }
  }
  for(i=1;i<numprocs;i++){ 
    int placeHolder=0;
        for(j=0;j<sizes[i];j++){ 
          //Sends data from master processor.
          MPI_Send(&allWords[(i-1)*sizes[i-1]+j],1,cWord,i,1,MPI_COMM_WORLD);
          placeHolder++;
        }
      }
  } 

else{
  for(i=0;i<tempSize;i++){
    MPI_Recv(&smallWords[i],1,cWord,MASTER_PROCESSOR,MPI_ANY_TAG,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
  }
  
  /*
    Sort all elements in given arrays. BubbleSort is used.
  */
    for(i=0;i<tempSize-1;i++){
      for(j=0;j<tempSize-1-i;j++){
        if(strcmp(smallWords[j].word,smallWords[j+1].word)>0){
          myWord temp=smallWords[j+1];
          strcpy(smallWords[j+1].word,smallWords[j].word);
          strcpy(smallWords[j].word,temp.word);
        }
      }
    }
    //Sends all words in sorted order.
    for(i=0;i<tempSize;i++){
      MPI_Send(&smallWords[i],1,cWord,MASTER_PROCESSOR,1,MPI_COMM_WORLD);
    }
}
  if(mypid==MASTER_PROCESSOR){
     for(i=1;i<numprocs;i++){
        for(j=0;j<sizes[i];j++){
        MPI_Recv(&receiver,1,cWord,i,MPI_ANY_TAG,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
          //Finds appropriate places for incoming elements in reduced list.
        if(used==0){
          //This statement is used because at least one element is required for comparison.
          reduced[used]=receiver;
          used++;
         }else{
          for(k=0;k<used;k++){
            if(strcmp(receiver.word,reduced[k].word)==0){
              reduced[k].occurrence+=1;
              break;
            }
          }
          if(k==used){
            reduced[used]=receiver;
            used++;
          }
         }
        }
  }
  /*
    Sorts all reduced words.
  */
     for(i=0;i<used-1;i++){
      for(j=0;j<used-1-i;j++){
        if(strcmp(reduced[j].word,reduced[j+1].word)>0){
          myWord temp=reduced[j+1];
          reduced[j+1]=reduced[j];
          reduced[j]=temp;
        }
      }
    }

  //Open file pointer.
  fp=fopen(argv[2],"w");
  int i;
  //Writes output.
  for(i=0;i<used;i++){
    fprintf(fp, "%s %d\n",reduced[i].word,reduced[i].occurrence);
  }
  //Close file pointer.
  fclose(fp);
    }
  //Frees user defined data type.
  MPI_Type_free(&cWord);
  //Finalize parallel environment.
  MPI_Finalize();
 
  exit(0);
}
