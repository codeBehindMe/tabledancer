
def find_first_between(s : str, token: str, start_index : int, finish_index : int) -> int:
  """Finds the index of first occurance of the token within some window within 
    the string.

  Args:
      s (str): String to search in.
      token (str): Token to find.
      start_index (int): Start index of window, greater than 0 and less than 
        len(s)
      finish_index (int): Finish index of window, greater than 0 and less than
        len(s)

  Returns:
      int: Index of the first found token within window or -1 if cannot be 
        found.
  """

  idx = 0
  while True:
    idx = s[idx: ].find(token) + idx
    if idx == -1:
      return -1

    if idx < start_index:
      idx = idx + 1
      continue

    if idx > finish_index:
      return -1 

    return idx
