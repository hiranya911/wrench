namespace java edu.ucsb.cs.wrench.messaging

struct BallotNumber {
  1:i64 number,
  2:string processId
}

service WrenchManagementService {

  bool ping(),

  bool election(),

  void victory(1:string processId),

  bool isLeader(),

  void prepare(1:BallotNumber bal, 2:i64 requestNumber),

  void ack(1:BallotNumber b, 2:map<i64,BallotNumber> acceptNumbers, 3:map<i64,string> acceptValues, 4:map<i64,string> outcomes),

  void accept(1:BallotNumber b, 2:i64 requestNumber, 3:string value),

  void accepted(1:BallotNumber bal, 2:i64 requestNumber),

  void decide(1:BallotNumber bal, 2:i64 requestNumber, 3:string command),

  bool append(1:string transactionId, 2:string data),

  void notifyAppend(1:string transactionId)
}

