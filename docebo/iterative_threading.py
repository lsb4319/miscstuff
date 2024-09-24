import concurrent.futures
import time

def square(n):
    """Function to square a number."""
    print(f"Squaring {n}")
    time.sleep(5)
    return n * n

def main():
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # Using ThreadPoolExecutor to process the list concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(square, numbers))
    
    # Display the squared results
    for num, squared in zip(numbers, results):
        print(f"{num} squared is {squared}")
        

if __name__ == "__main__":
    main()
