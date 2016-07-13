import logging

def create(name, stream, level):
    root = logging.getLogger(name)
    root.setLevel(level)

    ch = logging.StreamHandler(stream)
    ch.setLevel(level)
    formatter = logging.Formatter('%(levelname)-10s %(asctime)s %(message)s')

    ch.setFormatter(formatter)
    root.addHandler(ch)

    return root
