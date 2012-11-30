rm -rf gen-java
thrift -gen java WrenchManager.thrift
cp gen-java/edu/ucsb/cs/wrench/messaging/WrenchManagementService.java ../core/src/main/java/edu/ucsb/cs/wrench/messaging/WrenchManagementService.java 
