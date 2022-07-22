# Databricks notebook source
# MAGIC %md
# MAGIC # Mounting Azure Storage locations into the Databricks Filesystem (dbfs)
# MAGIC 
# MAGIC To mount a storage location, a cluster is required. But after this operation, mount locations are available to the workspace, and therefore accessible from other clusters. 
# MAGIC 
# MAGIC Mounting storage location is one of the options to work with data in azure databricks. See also topics below for alternatives.
# MAGIC 
# MAGIC Mounting requires storage access keys. To avoid hard-coding storage keys, configure secrets in a databricks or Azure KeyVault-backed secret scope (see separate notebook)
# MAGIC 
# MAGIC **Further Reading:**
# MAGIC - Control access to secrets: https://docs.microsoft.com/en-us/azure/databricks/security/access-control/secret-acl
# MAGIC - Access to Datalake with AD credential PassThrough: https://docs.microsoft.com/en-us/azure/databricks/security/credential-passthrough/adls-passthrough

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pre-requisites
# MAGIC work through the Secrets notebook first

# COMMAND ----------

#view existing mounts
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/lending-club-loan-stats

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets/foo')

# COMMAND ----------

# MAGIC %md
# MAGIC ### To unmount:

# COMMAND ----------

# unmount existing mounts (mounting in the location /mnt is only convention)

dbutils.fs.unmount("/mnt/sampledata")
dbutils.fs.unmount("/mnt/sampledata2")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Access Data with Secrets

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Mount blob storage containers

# COMMAND ----------

#Supply storageName and accessKey values
storageName = "moensembledata"
accessKey = "DG1JH+DzSNLxI4kKKPlu1wwOSXSopn69sMU0nYqbFptqJsNs8x3txu+DNACKoJUBskLKP/Lwt5a8+AStT2GnhA=="
container = "databrickscontainer"
mountpoint = "/mnt/databrickscontainer"

try:
  if any(mount.mountPoint == mountpoint for mount in dbutils.fs.mounts()):
    print('already mounted')
  else:
    dbutils.fs.mount(
      source = "wasbs://"+container+"@"+storageName+".blob.core.windows.net/",
      mount_point = mountpoint,
      extra_configs = {"fs.azure.account.key."+storageName+".blob.core.windows.net":
                       accessKey})
except Exception as e:
    print(e) # Otherwise print the whole stack trace.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount data lake zones

# COMMAND ----------

#Supply storageName and accessKey values
lakeName = "mydatalake"
lakeAccessKey = dbutils.secrets.get('my-secret-scope','datalake-pk')
container = "bronze"
mountpoint = "/mnt/bronze"

try:
  if any(mount.mountPoint == mountpoint for mount in dbutils.fs.mounts()):
    print('already mounted')
  else:
    dbutils.fs.mount(
      source = "wasbs://"+container+"@"+lakeName+".blob.core.windows.net/",
      mount_point = mountpoint,
      extra_configs = {"fs.azure.account.key."+lakeName+".blob.core.windows.net":
                       lakeAccessKey})
except Exception as e:
    print(e) # Otherwise print the whole stack trace.

# COMMAND ----------

dbutils.secrets.get('keyvault','storage-datalakeexplore-pk')

# COMMAND ----------

dbutils.fs.ls("/mnt/databrickscontainer")

# COMMAND ----------


