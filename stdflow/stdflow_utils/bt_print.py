from colorama import Fore, Style


def print_header(text: str, i: int = None, longest_worker_path_adjusted: int = None, min_blank: int = 10):
    if longest_worker_path_adjusted is None:
        longest_worker_path_adjusted = len(text)
    end = " " * 4
    start = f"    {i+1:02}." if i is not None else f"    __."
    separator_line_len = max(longest_worker_path_adjusted + len(start) + min_blank + len(end), 25)
    separator_line = Style.BRIGHT + "=" * separator_line_len + Style.RESET_ALL
    blank = separator_line_len - len(start) - len(text) - len(end)

    print(separator_line)
    print(Style.BRIGHT + f"{start}{blank * ' '}" + f"{text}" + Style.RESET_ALL)
    print(separator_line + Style.RESET_ALL)
