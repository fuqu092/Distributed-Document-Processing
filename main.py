from upload import upload_pdf

def main():
    print("Press 1 to upload new PDF.")
    print("Press 2 to search search .")

    option = input("Enter your input: ")

    if option == "1":
        pdf_name = input("Enter PDF file name: ").strip()

        result = upload_pdf(pdf_name)

        if result == False:
            print("Error uploading file!!!")
        else:
            print("File uploaded successfully.")

    elif option == "2":
        print("Feature coming soon !!!")
    else:
        print("Enter a valid input.")

if __name__ == "__main__":
    main()