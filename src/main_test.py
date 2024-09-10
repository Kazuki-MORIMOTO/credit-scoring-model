import os

def load_config():
    config_name = os.getenv('CONFIG', 'dev')  # デフォルトは 'dev'
    if config_name == 'prod':
        import conf.prod as config
    elif config_name == 'stg'
        import conf.stg as config
    else:
        import conf.dev as config
    return config

def main():
    config = load_config()
    print(f"bucket URL: {config.S3_BUCKET}")

if __name__ == "__main__":
    main()