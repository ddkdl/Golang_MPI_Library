package main
import (
    "fmt"
    "./MPI_Golang"
    "time"
)

/*****Global Variables*****/
var timeLoop time.Duration
var mypeer MPI_Golang.Peer;

/*This main function tests a simple gather on a world of peers*/
func main(){
  //***** Define Global Parameters ******//
  mypeer.IS = "149.166.26.94"  //defines IP for index server
  mypeer.Port = ":9999"        //defines port for TCP con
  mypeer.Data = MPI_Golang.IndexServer{
    ReqType: 2,
    MultiName: []string{"MyHC", "Other", "BMCJ"}} //defines the peers in the world
  timeLoop = 5 // defines the time in seconds for the heartbeat messages


  //****** Defines the message JSON struct ********//
  mypeer.Mess = MPI_Golang.Msg{
    MyType: "Int",     //defines the type of meesage to be passed
    MyString: "My Message from Other Server",
    MyInt: []int{1,2,3,4}}   //defines the string message

  /**** MPI Init ****/
  mypeer.Init()// Initializes the world of peers
  fmt.Println("my rank is", mypeer.Rank)

  /*** Global Communication Test****/
  // Scatters an int array, each peer alters values, then all values are gathered back
  go mypeer.SendBeatLoop(timeLoop); //begins the go routine for the heartbeat messages
  mypeer.Scatter(1, 2)   // Peer rank 1 Gathers all the messages
  if mypeer.Rank != 1{
    fmt.Println(mypeer.Mess.MyInt) //Prints gathered message string if rank 1
    for i := 0; i < len(mypeer.Mess.MyInt); i++{
      mypeer.Mess.MyInt[i] = mypeer.Mess.MyInt[i] + mypeer.Rank
    }
  }
  mypeer.Gather(1, "int")
  if mypeer.Rank == 1{
    fmt.Println("The new array is:", mypeer.Mess.MyInt)
  }

  /*** Broadcast ***/
  if mypeer.Rank == 1{
    mypeer.Mess.MyType = "String"
  }
  mypeer.BCast(1)
  fmt.Println("The broadcasted message is:", mypeer.Mess.MyString)

  /***** One-to-One Communcation ****/
  if mypeer.Rank == 0{
    mypeer.Mess.MyType = "int"
    mypeer.Send("BMCJ", 0)
  }
  if mypeer.Rank ==2{
    mypeer.Receive()
    fmt.Println("The received message is:", mypeer.Mess.MyInt)
  }

  /***** MPI Finalize ****/
  mypeer.Finalize()
}
