from dotenv import load_dotenv
load_dotenv()

from bronze_to_silver import create_silver


if __name__ == '__main__':
    
    create_silver()