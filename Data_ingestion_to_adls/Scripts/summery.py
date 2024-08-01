import re

def clean_text(input_file, output_file):
  """Removes numbers and empty lines containing only whitespace or special characters.

  Args:
    input_file: The path to the input file.
    output_file: The path to the output file.
  """

  with open(input_file, 'r') as f_in, open(output_file, 'w') as f_out:
    for line in f_in:
      # Remove numbers
      line = re.sub(r'\d+', '', line)
      line = re.sub(r':', '', line)
      # Remove leading and trailing whitespace
      line = line.strip()
      # Check if the line is empty after stripping
      if line:
        f_out.write(line + '\n')

if __name__ == '__main__':
  input_file = r'C:\Users\sahill\Desktop\Big data\CapstoneProject\transript.txt'
  output_file = r'C:\Users\sahill\Desktop\Big data\CapstoneProject\Data\output.txt'

  clean_text(input_file, output_file)
