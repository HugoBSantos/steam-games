from dotenv import load_dotenv
load_dotenv()

from src.bronze_to_silver import create_silver


if __name__ == '__main__':
    
    create_silver()