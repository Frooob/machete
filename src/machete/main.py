import pandas as pd
from connector.connector import Connection


def main():
    print("Hello from machete!")


if __name__ == "__main__":
    main()
    df = pd.DataFrame({1: [2]})
    c = Connection("Speedo", 2)
    c.get_connection()
