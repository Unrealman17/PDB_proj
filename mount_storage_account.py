# Databricks notebook source
dbutils.fs.mount(
  source = 'wasbs://test@evarganovstorage.blob.core.windows.net',
  mount_point= '/mnt/testblobstorage',
  extra_configs= {'fs.azure.account.key.evarganovstorage.blob.core.windows.net':
                      dbutils.secrets.get(scope = "MySecretScope", key = "ACCESS_KEY_STORAGE_ACCOUNT")}
)

# COMMAND ----------

#with open('//dbfs/mnt/testblobstorage/test.txt', 'w') as f:
#    f.write('1234567890')

# COMMAND ----------


