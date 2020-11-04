import json
import pyspark
import configFile
from pyspark.sql import functions
from pyspark.sql.functions import explode
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType

#Create Spark Session
sparkSession = pyspark.sql.SparkSession.builder.config(conf=configFile.sparkConfig).getOrCreate()

dfDatabaseData = sparkSession.read.option("multiline", "true")\
    .json(configFile.dbaasSourceFile)

str = StructType.fromJson(json.load(open("/Users/prammitr/Documents/Doc/my_projects/pyspark/dbaas_excra_schema.json")))

# 1. dbaas_db_system_dim
## Dependencies:
### $.dbSystemData  --> dbaas_db_system_dim
dfdbaas_db_system_dim = sparkSession.read.schema(str).option("multiline", "true")\
    .json(configFile.dbaasSourceFile)\
    .withColumn("dbSystemData", explode("dbSystemData")) \
    .select("dbSystemData.*")
dfdbaas_db_system_dim.createOrReplaceTempView("dbaas_db_system_dim")
dbaas_db_system_dim = sparkSession.sql(
     "select id,displayName,computeShape,dbSystemShape,databaseEdition,nodeCount,timeCreated,nodeCount,licenseType,tempHostSerial from dbaas_db_system_dim a")

#print("Printing Data for ---- > dbSystemData  --> dbaas_db_system_dim")
#dbaas_db_system_dim.show(2)


# 2. dbaas_db_node_fact
## Dependencies:
## 2.1 dbaas_db_node_stg -> $.dbNodeData
## 2.2 dbaas_db_host_stg -> $.dbHostData
## 2.3 dbaas_db_system_stg (same as dbaas_db_system_dim ) -> $.dbSystemData

# 2.1 dbNodeData --> dbaas_db_node_stg
dfdbNodeData = sparkSession.read.schema(str).option("multiline", "true")\
    .json(configFile.dbaasSourceFile) \
    .withColumn("dbNodeData", explode("dbNodeData")) \
    .select("dbNodeData.*")

dfdbNodeData.createOrReplaceTempView("dbaas_db_node_stg")
dbaas_db_node_stg = sparkSession.sql(
     "select b.* from dbaas_db_node_stg b")

#print("Printing Data for --> dfdbNodeDataTable  --> ocidwstg.dbaas_db_node_stg")
#dbaas_db_node_stg.printSchema()
#
# 2.2 dbHostData
dfdbHostData = sparkSession.read.schema(str).option("multiline", "true")\
    .json(configFile.dbaasSourceFile) \
    .withColumn("dbHostData", explode("dbHostData")) \
    .select("dbHostData.*")

dfdbHostData.createOrReplaceTempView("dbaas_db_host_stg")
dbaas_db_host_stg = sparkSession.sql(
     "select c.* from dbaas_db_host_stg c")

#print("Printing Data for --> dfdbHostDataTable  --> dbaas_db_host_stg")
#dbaas_db_host_stg.show(2)
#dbaas_db_host_stg.printSchema();

# 2.3  dbaas_db_system_stg --> $.dbSystemData
dfdbaas_db_system_dim = sparkSession.read.schema(str).option("multiline", "true")\
    .json(configFile.dbaasSourceFile)\
    .withColumn("dbSystemData", explode("dbSystemData")) \
    .select("dbSystemData.*")
dfdbaas_db_system_dim.createOrReplaceTempView("dbaas_db_system_stg")
dbaas_db_system_stg = sparkSession.sql(
     "select d.* from dbaas_db_system_stg d")

#print("Printing Data for ---- > dbSystemData  --> dbaas_db_system_stg")
#dbaas_db_system_stg.show(2)
##dbaas_db_system_stg.printSchema();


# 2.3 dbaas_db_system_stgd --> dbSystemData -->
dfdbSystemData = sparkSession.read.schema(str).option("multiline", "true")\
    .json(configFile.dbaasSourceFile)\
    .withColumn("dbSystemData", explode("dbSystemData")) \
    .select("dbSystemData.*")
dfdbSystemData.createOrReplaceTempView("dbaas_db_system_stg")
dbaas_db_system_stg = sparkSession.sql(
     "select e.* from dbaas_db_system_stg e")

#print("Printing Data for ---- > dbSystemData  --> ocidwstg.dbaas_db_system_stg")
#dbaas_db_system_stg.show(2)

# 2.4 dbaas_db_node_dim
## Dependencies:
## 2.4.1 dbaas_db_node_stg -> Already Done in 2.1
## 2.4.2 dbaas_db_system_dim --> Already Done in 2.3
## 2.4.3 dbaas_db_node_dim --> Ignoring due to snapshot logic
### Logic: dbaas_db_node_dim -> dbaas_db_node_stg

#  dbaas_db_node_dim <--> dbaas_db_node_stg
dfdbNodeData = sparkSession.read.schema(str).option("multiline", "true")\
    .json(configFile.dbaasSourceFile) \
    .withColumn("dbNodeData", explode("dbNodeData")) \
    .select("dbNodeData.*")

dfdbNodeData.createOrReplaceTempView("dbaas_db_node_dim")
dbaas_db_node_dim = sparkSession.sql(
     "select f.* from dbaas_db_node_dim f")

#print("Printing Data for --> dbaas_db_node_dim")
#dbaas_db_node_dim.show(2)



# 2.5 dbaas_db_host_dim
## Dependencies:
## 2.5.1 dbaas_db_host_stg -> Already done at #2.2 dbHostData
## 2.5.2 dbaas_db_node_dim -> Ignoring due to snapshot data
## 2.5.3 dbaas_db_host_dim -> Ignoring due to snapshot data

dbaas_db_host_dim = sparkSession.read.schema(str).option("multiline", "true")\
    .json(configFile.dbaasSourceFile) \
    .withColumn("dbHostData", explode("dbHostData")) \
    .select("dbHostData.*")

dbaas_db_host_dim.createOrReplaceTempView("dbaas_db_host_dim")
dbaas_db_host_dim = sparkSession.sql(
     "select g.* from dbaas_db_host_dim g")

#print("Printing Data for --> dbaas_db_host_dim")
#dbaas_db_host_dim.show(2)


# 2.6 dbaas_db_system_dim -> Already done # 1. dbaas_db_system_dim


################################################
dbaas_db_node_fact = sparkSession.sql("select\n" +
                                     # "    dbaas_db_node_stg.region,\n" +
                                     # "    dbaas_db_node_stg.ad,\n" +
                                     # "    dbaas_db_node_stg.state,\n" +
                                     # "    dbaas_db_system_stg.cpu_cores\n" +
                                     "dbaas_db_node_stg.*,\n" +
                                     "dbaas_db_host_stg.*,\n" +
                                     "dbaas_db_system_stg.*\n" +
                                     "from\n" +
                                     "    dbaas_db_node_stg,\n" +
                                     "    dbaas_db_host_stg,\n" +
                                     "    dbaas_db_system_stg\n");
                                     # "where\n" +
                                     # "dbaas_db_node_stg.db_node_id == dbaas_db_host_stg.db_node_id\n"
                                     # "AND dbaas_db_node_stg.db_host_id == dbaas_db_host_stg.db_host_id\n"
                                     # "AND dbaas_db_node_stg.db_sys_id == dbaas_db_system_stg.db_sys_id\n"
                                     # );

dbaas_db_node_fact.createOrReplaceTempView("dbaas_db_node_fact_v");
dbaas_db_node_fact_v = sparkSession.sql(
      "select l.* from dbaas_db_node_fact_v l")

#print("Printing Data for ---- > dbaas_db_node_fact_v")
#dbaas_db_node_fact_v.show(2)

################################################



# 4  dbaas_db_home_dim
## Dependencies:
## 4.1 dbaas_db_home_stg -> dbHomeData
## 4.2 dbaas_db_system_dim -> One to one mapping with dbaas_db_system_stg

#4.1 dbaas_db_home_stg
dbaas_db_home_stg = sparkSession.read.schema(str).option("multiline", "true")\
    .json(configFile.dbaasSourceFile)\
    .withColumn("dbHomeData", explode("dbHomeData")) \
    .select("dbHomeData.*")
dbaas_db_home_stg.createOrReplaceTempView("dbaas_db_home_stg")
dbaas_db_home_stg = sparkSession.sql(
     "select h.* from dbaas_db_home_stg h")

#print("Printing Data for ---- > dbHomeData  --> dbaas_db_home_stg")
#dbaas_db_home_stg.show(2)
#dbaas_db_home_stg.printSchema();

#4.2 dbaas_db_system_dim : Already done in # 2.3  dbaas_db_system_stg --> $.dbSystemData
dbaas_db_system_dim = sparkSession.read.schema(str).option("multiline", "true")\
    .json(configFile.dbaasSourceFile)\
    .withColumn("dbSystemData", explode("dbSystemData")) \
    .select("dbSystemData.*")
dbaas_db_system_dim.createOrReplaceTempView("dbaas_db_system_dim")
dbaas_db_system_dim = sparkSession.sql(
     "select i.* from dbaas_db_system_dim i")

#print("Printing Data for ---- > dbSystemData  --> dbaas_db_system_dim")
#dbaas_db_system_dim.show(2)
#dbaas_db_system_dim.printSchema();

#4
dbaas_db_home_dim = sparkSession.sql("select\n" +
                                     "    dbaas_db_home_stg.compartmentId,\n" +
                                     "    dbaas_db_home_stg.dbVersion,\n" +
                                     "    dbaas_db_home_stg.displayName,\n" +
                                     "    dbaas_db_home_stg.id,\n" +
                                     "    dbaas_db_home_stg.lifecycleState,\n" +
                                     "    dbaas_db_home_stg.timeCreated,\n" +
                                     "    dbaas_db_home_stg.timeUpdated,\n" +
                                     "    dbaas_db_home_stg.dbSystemId\n" +
                                     "from\n" +
                                     "    dbaas_db_home_stg\n" +
                                     "    join dbaas_db_system_dim on\n" +
                                     "dbaas_db_home_stg.compartmentId == dbaas_db_system_dim.compartmentId");

dbaas_db_home_dim.createOrReplaceTempView("dbaas_db_home_dim_v")
dbaas_db_home_dim_v = sparkSession.sql(
      "select k.* from dbaas_db_home_dim_v k")

#print("Printing Data for ---- > dbaas_db_home_dim")
#dbaas_db_home_dim_v.show(2)

#5 exa_cc_compute_shape_dim (static data 37 records)
dfexa_cc_compute_shape_dim = sparkSession.read.options(header='True', inferSchema='True', delimiter=',') \
  .csv(configFile.dbaas_exa_dimFile)


dfexa_cc_compute_shape_dim.createOrReplaceTempView("dfexa_cc_compute_shape_dimTable")
dfexa_cc_compute_shape_dimTable = sparkSession.sql(
     "select service,dbaas_service,hardware_type,shape_type,shape,hardware_generation,max_cores,multiplier,db_sys_shape from dfexa_cc_compute_shape_dimTable a")

# print("Printing Data for ---- > dfexa_cc_compute_shape_dimTable")
# dfexa_cc_compute_shape_dimTable.show(2)


dbaas_db_node_fact = sparkSession.sql("select\n" +
                                        " a.lifecycleState as state, \n" +
                                        " b.lifecycleState as state,\n" +
                                        " a.timeUpdated as time_updated, \n" +
                                        " c.lifecycleState  as region, \n" +
                                        " c.cpuCoreCount as cpu_cores, \n" +
                                        " b.agentCpuCoreCount as agent_cpu_cores, \n" +
                                        " c.timeUpdated as time_updated, \n" +
                                        " c.timeUpdated as time_updated, \n" +
                                        " c.tenantId as tenantId, \n" +
                                        " c.internalClusterName as internalClusterName, \n" +
                                        " c.id as db_sys_id, \n" +
                                        " c.displayName as db_sys_name, \n" +
                                        " c.computeShape as compute_shape, \n" +
                                        " c.dbSystemShape as db_sys_shape, \n" +
                                        " c.databaseEdition as databaseEdition, \n" +
                                        " c.nodeCount as nodeCount, \n" +
                                        " c.timeCreated as timeCreated, \n" +
                                        " c.licenseType as licenseType \n" +
                                        " from dbaas_db_node_stg a,\n" +
                                        " dbaas_db_host_stg b,\n"
                                        " dbaas_db_system_stg c\n"
                                        " WHERE a.dbHostId = b.id\n");

dbaas_db_node_fact.createOrReplaceTempView("dbaas_db_node_fact_Table")
#dbaas_db_node_fact.show(1)
#dbaas_db_node_fact.printSchema();

dfMetaData = sparkSession.read.option("multiline", "true") \
    .json(configFile.dbaasSourceFile) \
    .select("metadata.*")

dfMetaData.createOrReplaceTempView("dfMetaData_Table")
#dfMetaData.show(2)

dbaas_join = sparkSession.sql("select\n" +
                                        " m.creationDate as record_date, \n" +
                                        # category ## ignoring as coming from account_dim
                                        # company_name ## ignoring as coming from account_dim
                                        # common_name ## ignoring as coming from account_dim
                                        # account_name ## ignoring as coming from accounts
                                        " n.tenantId, \n" +
                                        " n.internalClusterName, \n" +
                                        " n.region, \n" +
                                        # SUBSTR(n.ad,INSTR(n.ad,'-')+1,4) AS AD,
                                        " n.db_sys_id, \n" +
                                        " n.db_sys_name, \n" +
                                        ## db_sys_key, ## ignoring as this is DWH property
                                        " n.compute_shape, \n" +
                                        " n.db_sys_shape, \n" +
                                        " sh.service, \n" +
                                        " sh.dbaas_service, \n" +
                                        " sh.hardware_type as hardware, \n" +
                                        " sh.shape_type as prod_service, \n" +
                                        " sh.shape, \n" +
                                        " sh.hardware_generation, \n" +
                                        " n.databaseEdition as db_edition, \n" +
                                        #yy.db_version ## ignoring as coming from dimension tables
                                        #SUBSTR(yy.db_version, 1, 4) as db_short_version, ## ignoring as coming from dimension tables
                                        ## To do
                                        # s.initial_cpu_cores as initial_cpu_system_dim_cores,
                                        " n.nodeCount as allocated_cores, \n" +
                                        " case \n"
                                        " when sh.dbaas_service = 'DB' then n.nodeCount * sh.max_cores \n" +
                                        " when sh.dbaas_service = 'Exadata' then sh.max_cores \n" +
                                        " else 0 \n" +
                                        " end as max_cores, \n" +
                                        ## To do
                                        # coalesce(s.node_count * n.actual_cpu_cores, s.initial_cpu_cores) as db_cores,
                                        " sh.multiplier as exa_racks, \n" +
                                        " n.licenseType, \n" +
                                        " n.timeCreated, \n" +
                                        " n.cpu_cores \n" +
                                        " from dbaas_db_node_fact_Table n, \n" +
                                        " dfexa_cc_compute_shape_dimTable sh, \n" +
                                        " dfMetaData_Table m \n" +
                                        " WHERE sh.db_sys_shape = n.db_sys_shape\n");

dbaas_join.createOrReplaceTempView("dbaas_join_Table")
dbaas_join.show(1)
dbaas_join.repartition(1).write.mode("overwrite").csv(configFile.dbaasReport1File)






