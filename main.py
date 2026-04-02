from dotenv import load_dotenv
load_dotenv()

from src.data_modeling import create_silver


if __name__ == '__main__':
    
    create_silver()