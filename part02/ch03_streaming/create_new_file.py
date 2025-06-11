import os

if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.abspath(__file__))

    stocks_file_path = os.path.join(base_dir, "data", "stocks")

    for i in range(10):
        file_name = f"{stocks_file_path}/{i}.csv"

        with open(file_name, "w") as file:
            data = "AAPL,2023.3.15,153.25"
            file.write(data)