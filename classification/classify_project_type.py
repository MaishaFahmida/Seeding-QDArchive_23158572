QDA_EXTENSIONS = {
    ".qdpx",
    ".mx24",
}

PRIMARY_DATA_EXTENSIONS = {
    ".pdf",
    ".rtf",
    ".txt",
    ".doc",
    ".docx",
}

IGNORE_EXTENSIONS = {
    ".htm",
    ".html",
}


def classify_project_type(file_types):
    """file_types: list of file_type strings (e.g. ['.pdf', '.xml']) for SUCCEEDED files only"""
    extensions = {ft.lower() for ft in file_types if ft}

    if extensions & QDA_EXTENSIONS:
        return "QDA_PROJECT"

    if extensions & PRIMARY_DATA_EXTENSIONS:
        return "QD_PROJECT"

    valid_extensions = extensions - IGNORE_EXTENSIONS
    if valid_extensions:
        return "OTHER_PROJECT"

    return "NOT_A_PROJECT"