import os

def collect_files_and_folders(root_dir, output_file="output.txt"):
    """
    Collects all files and folders in a given directory (excluding .git),
    appends file content with folder names, and saves to a text file.

    Args:
        root_dir (str): The path to the root directory to scan.
        output_file (str): The name of the output text file.
    """
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for dirpath, dirnames, filenames in os.walk(root_dir):
            # Exclude .git directory from traversal
            if '.git' in dirnames:
                dirnames.remove('.git')

            # Write folder name to output file
            outfile.write(f"--- Folder: {os.path.relpath(dirpath, root_dir)} ---\n")

            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as infile:
                        content = infile.read()
                    outfile.write(f"--- File: {os.path.relpath(filepath, root_dir)} ---\n")
                    outfile.write(content)
                    outfile.write("\n\n")  # Add some separation between files
                except Exception as e:
                    outfile.write(f"--- Could not read file: {os.path.relpath(filepath, root_dir)} - Error: {e} ---\n\n")

# Example usage:
if __name__ == "__main__":
    target_directory = "D:\Transaction-Master"  # Replace with the actual directory path
    collect_files_and_folders(target_directory, "collected_code.txt")
    print(f"All files and folders (excluding .git) have been collected into 'collected_code.txt'")