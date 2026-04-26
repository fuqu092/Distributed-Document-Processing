from upload import upload_pdf
import os

def main():
    print("Press 1 to upload new PDF.", flush=True)
    print("Press 2 to search search .", flush=True)

    folder = input("Enter the folder path: ")

    for pdf in os.listdir(folder):
       upload_pdf(f'{folder}/{pdf}')

if __name__ == "__main__":
    main()