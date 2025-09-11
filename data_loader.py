from ingestion_scripts.move_api_to_azure import *


def main():
    q = get_and_move_data()
    if q:
        print("Success")
    else:
        print("Fail")

main()