def yield_partials(path):
    partial_path = ""
    for node in path.split("/"):
        if node == "":
            continue
        partial_path += "/" + node
        yield partial_path
