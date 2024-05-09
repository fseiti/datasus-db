import datasus_db
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

def main():
    datasus_db.import_sia_pa(years=[2023],states=["RR"], months=[4])




if __name__ == "__main__":
    main()