"""Git Exercise - Python Project.

This script is intended for educational purposes. It allows students to practice Git basics such as
cloning, branching, committing, solving conflicts, and more.

Students:
  - Gaspard
  - Iulia
  - Toguiyeni
  - Thomas

Instructions:
  - Each student should modify their assigned function according to the provided guidelines.
  - Create a branch with your name before modifying the code.
  - Commit your changes and push them to the repository.
  - Try to create conflicts by modifying shared parts of the code (e.g., the main function).
  - Resolve conflicts collaboratively.
  - If you use packages, make sure to include them in the requirements.txt file!

"""


def greet_user(name: str) -> str:
    """Function assigned to Thomas.

    Task: Improve this function by making the greeting more personalized.
    For example, allow the user to input their favorite hobby and include it in the greeting.

    Args:
        name (str): The name of the user.

    Returns:
        str: A greeting message.
    """
    return f"Hello, {name}! Welcome to the Git practice project."


def calculate_square(number: int) -> int:
    """Function assigned to Toguiyeni.

    Task: Modify this function to support calculating squares of floats as well.
    Additionally, implement error handling for invalid inputs.

    Args:
        number (int): A number to be squared.

    Returns:
        int: The square of the input number.
    """
    return number**2


def reverse_string(text: str) -> str:
    """Function assigned to Gaspard.

    Task: Modify this function to support reversing sentences by words, not just characters.
    Additionally, implement an option to preserve punctuation in the correct places.

    Args:
        text (str): A string to be reversed.

    Returns:
        str: The reversed string.
    """
    return text[::-1]


def factorial(n: int) -> int:
    """Function assigned to Iulia.

    Task: Modify this function to use recursion instead of a loop.
    Additionally, handle edge cases such as negative numbers or non-integer inputs.

    Args:
        n (int): A non-negative integer.

    Returns:
        int: The factorial of the input number.
    """
    result = 1
    for i in range(1, n + 1):
        result *= i
    return result


def main():
    """Main function to demonstrate all functionalities.

    Students may modify this function to test their changes.
    """
    print(greet_user("Student"))
    print(f"Square of 4: {calculate_square(4)}")
    print(f"Reversed string: {reverse_string('Hello World!')}")
    print(f"Factorial of 5: {factorial(5)}")


if __name__ == "__main__":
    main()
