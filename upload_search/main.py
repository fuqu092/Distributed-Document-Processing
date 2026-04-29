from upload import upload_pdf
from search import search

def main():
    print("Press 1 to upload new PDF.", flush=True)
    print("Press 2 to search search .", flush=True)

    option = input("Enter your input: ")

    if option == "1":
        pdf_name = input("Enter PDF file name: ").strip()

        result = upload_pdf(pdf_name)

        if result == False:
            print("Error uploading file!!!", flush=True)
        else:
            print("File uploaded successfully.", flush=True)

    elif option == "2":
        query = input("Enter your query: ")
        num_results = int(input("Enter the maximum number of results you want: "))
        search(num_results, query)

    else:
        print("Enter a valid input.", flush=True)

if __name__ == "__main__":
    main()