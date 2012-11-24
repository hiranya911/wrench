namespace java edu.ucsb.cs.wrench.messaging

service WrenchManagementService {

  bool election(),

  void victory(1:string processId)

}

