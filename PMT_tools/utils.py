import os


def makePath(in_folder, *subnames):
    """Dynamically set a path (e.g., for iteratively referencing
        year-specific geodatabases)
    Args:
        in_folder (str): String or Path
        subnames (list/tuple): A list of arguments to join in making the full path
            `{in_folder}/{subname_1}/.../{subname_n}
    Returns:
        Path
    """
    return os.path.join(in_folder, *subnames)


def validate_directory(directory):
    if os.path.isdir(directory):
        return directory
    else:
        try:
            os.makedirs(directory)
            return directory
        except:
            raise