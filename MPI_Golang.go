package MPI_Golang

/************ Golang Hybrid P2P Library *****************/

/*
This library defines the essential communication protocols for
parallel message passing. Its inteded use is for a hybrid P2P
enviroment. Comprised in this library are the following functions:
Initialize, Send, Receive, Gather, Scatter, and Broadcast, along
with the function Heartbeat message for the Index Server.
*/

import (
    "fmt"
    "net"
    "encoding/json"
    "gopkg.in/mgo.v2/bson"
    "time"
    "log"
    "strings"
)

/************************************
Peer Class for P2P Message Passing
************************************/
type Peer struct{
  Name string // User name
  IS string  // Index Server
  ID string // User ID
  IP string // User IP
  Port string // specifies port for communication
  Final bool
  Rank int
  Data IndexServer
  Mess Msg
}

/************************************
JSON struct that is sent and recieved
in a P2P MPI environment. The message
can pass either a String or Int
************************************/
type Msg struct{
  MyType string   //defines the data type of the message
  MyString string
  MyInt []int
}

/************************************
JSON struct used to query the IS.
This query could be for a peer
request or a hearbeat status update.
************************************/
type IndexServer struct{
  ReqType int // reqtype 0 client query by code
              //reqtype 2 client query by name
              // reqtype 4 heartbeat message to IS
  Name string //Name of peer
  MultiName []string // Name of peers in world
  MultiIpAddr []string // IP's of peers
  ID bson.ObjectId `bson:"_id,omitempty"`
  Timestamp time.Time
}

/**********Initialization************/
/*************************************
This method returns with a list of
IP's from the IS. This function is
also responsible for ranking the peers
in the world.
*************************************/
func( c *Peer) Init(){
  c.IP = getLocalIP() // Gets the local IP of the peer
  con, err := net.Dial("tcp", c.IS+c.Port) // Establish connection with IS
  errHandler(err)
  b,err := json.Marshal(c.Data) // Marshals the JSON struct
  errHandler(err)
  err = json.NewEncoder(con).Encode(b) // Encode the byte stream to IS
  errHandler(err)
  var msg []byte //byte stream for the decoding
  err = json.NewDecoder(con).Decode(&msg)
  errHandler(err)
  err = json.Unmarshal(msg,&c.Data) //Decode and unmarshal struct from IS
  errHandler(err)
  for i := 0; i < len(c.Data.MultiIpAddr); i++{ //Loop that ranks all the peers
    if c.Data.MultiIpAddr[i] == c.IP{
      c.Rank = i
    }
  }
}

/***************Broadcast***************/
/****************************************
This method is resonsible for sending a
message to all the peers in the world. It
accepts the broadcaster rank as input.
****************************************/
func( c *Peer) BCast(Broadcaster int){
  if c.Rank == Broadcaster{ // Checks to see which peer does the broadcasting

    for i := 0; i < len(c.Data.MultiIpAddr); i++{ // iterates through all the IP's of the peers
      if i != c.Rank{ // this check is to ensure the broadcaster does not broadcast to itself.
        con, err := net.Dial("tcp", c.Data.MultiIpAddr[i]+c.Port) // Establish connection with IS
        errHandler(err)
        b,err := json.Marshal(c.Mess) // Marshals the JSON struct
        errHandler(err)
        err = json.NewEncoder(con).Encode(b) // Encode the byte stream to IS
        errHandler(err)
        con.Close() // closes TCP connection
      }
    }
  }else{ // if not the broadcaster, listen for an incoming message
    ln, err := net.Listen("tcp", c.Port) // listen on a specific port
    errHandler(err)
    for {
      con, err := ln.Accept()// accept the TCP connection
      if err != nil {
        fmt.Println(err)
        continue
      }
      flag,er,e,Mess := c.receivedMsg(con) // Received a message
      con.Close()
      if(flag){
        ln.Close()
        if Mess.MyType == "int" || Mess.MyType == "Int"{
          c.Mess.MyInt = Mess.MyInt
        }
        if Mess.MyType == "string" || Mess.MyType == "String"{
          c.Mess.MyString = Mess.MyString
        }
        break
      }else{
        fmt.Println(er,e)
        ln.Close()
      }
    }
  }
}

/****************Scatter****************/
/****************************************
This method is responsible for scattering
a message to all the peers in the world.
Each peer receives a unique piece. It
accepts a scatterer and amount as input.
****************************************/
func( c *Peer) Scatter(Scatterer int, amount int){
  if c.Rank == Scatterer{ // Checks to see which peer does the scattering
    count := 0
    if c.Mess.MyType == "string" || c.Mess.MyType == "String"{ // if the message is a string
      temp := c.Mess.MyString // Creates a copy of the string
      c.Mess.MyString = ""
      for i := 0; i < len(c.Data.MultiIpAddr); i++{ //iterates through all the peer's IP's
        if i != c.Rank{
          count = count + amount // keeps track of the appropriate string index
          c.Mess.MyString = temp[count-amount:count] // Updates the string in the JSON struct with the appropriate segment
          con, err := net.Dial("tcp", c.Data.MultiIpAddr[i]+c.Port) // Establish connection with IS
          errHandler(err)
          b,err := json.Marshal(c.Mess) // Marshals the JSON struct
          errHandler(err)
          err = json.NewEncoder(con).Encode(b) // Encode the byte stream to IS
          errHandler(err)
          con.Close() // closes the TCP connection
        }
      }
    }
    if c.Mess.MyType == "int" || c.Mess.MyType == "Int"{ // if message sent is an int array
      temp := c.Mess.MyInt // creates a copy of the array
      c.Mess.MyInt = []int{}
      for i := 0; i < len(c.Data.MultiIpAddr); i++{
        if i != c.Rank{
          count = count + amount //keeps track of the appropriate index
          c.Mess.MyInt = temp[count-amount:count]
          con, err := net.Dial("tcp", c.Data.MultiIpAddr[i]+c.Port) // Establish connection with IS
          errHandler(err)
          b,err := json.Marshal(c.Mess) // Marshals the JSON struct
          errHandler(err)
          err = json.NewEncoder(con).Encode(b) // Encode the byte stream to IS
          errHandler(err)
          con.Close() // closes the TCP connection
        }
      }
    }
  }else{ // if peer rank is not the scatterer
    ln, err := net.Listen("tcp", c.Port) // listen on a specific port
    errHandler(err)
    for {
      con, err := ln.Accept()// accept a connection
      if err != nil {
        fmt.Println(err)
        continue
      }
      flag,er,e,Mess := c.receivedMsg(con) // Received a message
      con.Close()
      if(flag){
        ln.Close()
        if Mess.MyType == "int" || Mess.MyType == "Int"{
          c.Mess.MyInt = Mess.MyInt
        }
        if Mess.MyType == "string" || Mess.MyType == "String"{
          c.Mess.MyString = Mess.MyString
        }
        break
      }else{
        fmt.Println(er,e)
        ln.Close()
      }
    }
  }
}

/****************Gather****************/
/***************************************
This method is resonsible for collecting
a unique message segment from each peer
in the world. The gather appends the
final message together. Takes the
gatherer rank and type of message as
input.
***************************************/
func (c *Peer) Gather(Gatherer int, Type string){
  if c.Rank == Gatherer{ // checks to see which peer is the gatherer
    if Type == "string" || Type =="String"{
      temp := make([]string, len(c.Data.MultiIpAddr), len(c.Data.MultiIpAddr))
      c.Mess.MyString =""
      for i := 0; i < len(c.Data.MultiIpAddr); i++{
        if i != c.Rank{
          ln, err := net.Listen("tcp", c.Port) // listen on a specific port
          errHandler(err)
          for {
            con, err := ln.Accept()// accept a connection
            incomingIP := con.RemoteAddr().String() //uses the IP to discern which piece to send
            tempIP := strings.Split(incomingIP, ":") // splits the string in two. 0 corresponding to IP and 1 corresponding to port
            incomingIP = tempIP[0] // selects the IP string
            var incomingRank int
            for i := 0; i < len(c.Data.MultiIpAddr); i++{
              if incomingIP == c.Data.MultiIpAddr[i]{
                incomingRank = i // finds the incoming rank of the peer establishing the TCP connection
              }
            }
            if err != nil {
              fmt.Println(err)
              continue
            }
            flag,er,e,Mess := c.receivedMsg(con) // Received a message
            con.Close()
            if(flag){
              ln.Close()
              temp[incomingRank] = Mess.MyString // fills the string in a temp variable
              break
            }else{
              fmt.Println(er,e)
              ln.Close()
            }
          }
        }
      }
      for i :=0; i<len(temp); i++{
        c.Mess.MyString +=temp[i] // all temp variables are concatenated together for the final output
      }
    }
    if Type == "int" || Type == "Int"{ // if data type is an int
      temp := make([][]int, len(c.Data.MultiIpAddr), len(c.Data.MultiIpAddr))
      c.Mess.MyInt = []int{}
      for i := 0; i < len(c.Data.MultiIpAddr); i++{
        if i != c.Rank{
          ln, err := net.Listen("tcp", c.Port) // listen on a specific port
          errHandler(err)
          for {
            con, err := ln.Accept()// accept a connection
            incomingIP := con.RemoteAddr().String() //uses the IP to discern which piece to send
            tempIP := strings.Split(incomingIP, ":") // splits the string in two. 0 corresponding to IP and 1 corresponding to port
            incomingIP = tempIP[0] // selects the IP string
            var incomingRank int
            for i := 0; i < len(c.Data.MultiIpAddr); i++{
              if incomingIP == c.Data.MultiIpAddr[i]{
                incomingRank = i // finds the incoming rank of the peer establishing the TCP connection
              }
            }
            if err != nil {
              fmt.Println(err)
              continue
            }
            flag,er,e,Mess := c.receivedMsg(con) // Received a message
            con.Close()
            if(flag){
              ln.Close()
              temp[incomingRank] = Mess.MyInt // fills the string in a temp variable
              break
            }else{
              fmt.Println(er,e)
              ln.Close()
            }
          }
        }
      }
      for i :=0; i<len(temp); i++{
        c.Mess.MyInt = append(c.Mess.MyInt,temp[i]...) // all temp variables are concatenated together for the final output
      }
    }
  } else{
    con, err := net.Dial("tcp", c.Data.MultiIpAddr[Gatherer]+c.Port) // Establish connection with IS
    errHandler(err)
    b,err := json.Marshal(c.Mess) // Marshals the JSON struct
    errHandler(err)
    err = json.NewEncoder(con).Encode(b) // Encode the byte stream to IS
    errHandler(err)
    con.Close() //closes the connection
  }
}


/************Send***************/
/********************************
This function sends a JSON struct
to another peer. The message is
sent along a TCP connection.
********************************/
func( c *Peer) Send(Dest string, reqType int){
  switch reqType { // Case structure: 0 - string contains Name, 1 - string contains Code
    case 0:
      for i:=0; i < len(c.Data.MultiName); i++{ // finds the name of the Peer of interest
        if Dest == c.Data.MultiName[i]{
          con, err := net.Dial("tcp", c.Data.MultiIpAddr[i] + c.Port) // Establish the TCP connection with the peer of interest
          errHandler(err)
          b,e := json.Marshal(c.Mess) // Marshals and encodes message to peer
          errHandler(e)
          err = json.NewEncoder(con).Encode(b)
          errHandler(err)
          con.Close() // close connection
        }
      }
    case 1:
      fmt.Println("2")
      c.Data.ID = bson.ObjectIdHex(Dest) // bson for the ObjectId of the peer
  }
}

/**************Receive***************/
/*************************************
This function is responsible for
listening for a JSON struct on a
specified port.
*************************************/
func (p *Peer)Receive(){
  ln, err := net.Listen("tcp", p.Port) // listen on a specific port
  errHandler(err)
  for {
    c, err := ln.Accept()// accept a connection
    if err != nil {
      fmt.Println(err)
      continue
    }
    flag,er,e,Mess := p.receivedMsg(c) // Received a message
    if(flag){
      if Mess.MyType == "int" || Mess.MyType == "Int"{
          p.Mess.MyInt = Mess.MyInt
        }
      if Mess.MyType == "string" || Mess.MyType == "String"{
        p.Mess.MyString = Mess.MyString
      }
      ln.Close()
      break;
    }else{
      fmt.Println(er,e)
      ln.Close()
    }
  }
}

/***********************************
Receives message from peer
***********************************/
func (p *Peer)receivedMsg(c net.Conn)(bool,error,error,Msg){
  var msg []byte
  var Messy Msg
    err := json.NewDecoder(c).Decode(&msg) //Decode and Unmarshall the JSON message
    e := json.Unmarshal(msg,&Messy)
    c.Close();
    if(err==nil && e == nil){
      return true,nil,nil,Messy;
    }else{
      return false,e,err,Messy;
    }
}

/*************Finalize**************/
/************************************
Terminates the MPI world.
************************************/
func (p *Peer)Finalize(){
    p.Final = false; // terminates the hearbeat messages for that peer
    p.Data.MultiIpAddr = []string{} // remove all the stored IP's of the peers in the world
    p.Rank = -1 // makes the rank of the peer null
}

/************Heartbeat**************/
/************************************
Method for setting the interval for
heartbeat messages to the IS. A time
duration is the input.
************************************/
func (p *Peer)SendBeatLoop(timeInterval time.Duration){
  p.Final = true // starts the continuous heartbeat messages
  for (p.Final) {
    p.SendBeat() // Calls heartbeat function
    time.Sleep(time.Second * timeInterval)
  }
}

/***********************************
Method for sending the heartbeat to
the IS
***********************************/
func (p *Peer)SendBeat(){
  var ISStruct IndexServer
  fmt.Println(time.Now()) //Prints time of heartbeat sent
  con, err := net.Dial("tcp", p.IS + p.Port) // Establishes the connection with the IS
  errHandler(err)
  ISStruct.Timestamp = time.Now()
  ISStruct.ReqType = 4 // reqtype of 4 signals a hearbeat message
  b,err := json.Marshal(ISStruct) // Marshals the JSON struct
  errHandler(err)
  err = json.NewEncoder(con).Encode(b) // Encode the byte stream
  errHandler(err)
}

/************Error Handler*************
/**************************************
Function that logs any errors
**************************************/
func errHandler(err error) {
  if err != nil {
        log.Fatal(err)
    }
}

/*************************************
This function returns the local IP of
the peer
*************************************/
func getLocalIP() string {
  addrs, err := net.InterfaceAddrs()
  if err != nil {
      return ""
  }
  for _, address := range addrs {
      // check the address type and if it is not a loopback the display it
      if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
          if ipnet.IP.To4() != nil {
              return ipnet.IP.String() // returns the IP address
          }
      }
  }
  return ""
}
