from argparse import ArgumentParser

from src.client import Client


if __name__ == "__main__":
    parse = ArgumentParser()
    parse.add_argument('-p', '--port', default=5000, type=int, help='port to listen on')
    #parse.add_argument('-f', '--folder', default=None, type=str, help='folder to read images from')
    args = parse.parse_args()


    c = Client(args.port)
    c.loop() 
