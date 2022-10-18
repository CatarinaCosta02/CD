from argparse import ArgumentParser

from src.daemon import daemon

if __name__ == "__main__":
    parse = ArgumentParser()
    #parse.add_argument('-p', '--port', default=5000, type=int, help='port to listen on')
    #parse.add_argument('-s', '--super', default=None, type=int, help='port to start process')
    parse.add_argument('-f', '--folder', default=None, type=str, help='folder to read images from')
    args = parse.parse_args()

    broker = daemon(args.folder)
    broker.run() 
