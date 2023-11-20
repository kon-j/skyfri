from src.utils import set_up_database
from src.config import (BATCH_SIZE, DB_NAME, 
                        INPUT_FOLDER_NAME, FILE_NAME_PAIRS, FILE_NAME_DURATION)
from src.reporter import Reporter



if __name__ == "__main__":
    set_up_database(DB_NAME)
    reporter = Reporter(DB_NAME,
                        INPUT_FOLDER_NAME,
                        BATCH_SIZE,
                        FILE_NAME_DURATION,
                        FILE_NAME_PAIRS)
    reporter.load_data()
    reporter.prepare_reports()

