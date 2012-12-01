namespace java edu.ucsb.cs.wrench.messaging

struct BallotNumber {
  1:i64 number,
  2:string processId
}

struct DatabaseSnapshot {
  1:list<string> grades,
  2:list<string> stats
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

  void notifyPrepare(1:string transactionId),

  map<i64,string> getPastOutcomes(1:i64 lastRequest),

  BallotNumber getNextBallot(),

  bool notifyCommit(1:string transactionId, 2:i64 lineNumber),

  DatabaseSnapshot read(),

  list<string> getLines(1:i32 lineCount)

}

