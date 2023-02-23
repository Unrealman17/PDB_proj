from pdb_helper import read_config


def mount_storage_account():
    storage_account_mount_point = read_config()["storage_account_mount_point"]

    if storage_account_mount_point not in [m.mountPoint for m in dbutils.fs.mounts()]:
        print(f'mounting {storage_account_mount_point} . . .')
        dbutils.fs.mount(
            source='wasbs://test@evarganovstorage.blob.core.windows.net',
            mount_point='/mnt/testblobstorage',
            extra_configs={'fs.azure.account.key.evarganovstorage.blob.core.windows.net':
                           dbutils.secrets.get(scope="MySecretScope", key="ACCESS_KEY_STORAGE_ACCOUNT")}
        )
        print('Done!')
    else:
        print(f'{storage_account_mount_point} already mounted.')


if __name__ == "__main__":
    mount_storage_account()
