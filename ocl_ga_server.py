#!/usr/bin/python3
import traceback
import socket
import threading

# alternative implementation:
# https://github.com/python/asyncio/blob/master/examples/simple_tcp_server.py
# https://docs.python.org/3.6/library/asyncio.html

# internal server thread
class ServerThread(threading.Thread):
    def __init__(self, server_socket, callbacks):
        threading.Thread.__init__(self)
        self.__dataLock = threading.Lock()
        self.__server = server_socket
        self.__clients = []
        self.__callbacks = callbacks;
        self.__dataStore = []

    def run(self):
        # we are trying to use single thread to process all operations
        self.__server.listen()
        self.__server.settimeout(0.01)

        while True:
            try:
                # wait clients for 0.1 secs
                client_info = self.__server.accept()
                self.__clients.append(client_info)
                # use non-blocking mode
                client_info[0].settimeout(0.01)
                if "connected" in self.__callbacks:
                    print("client connected from: %s"%(client_info[1]))
                    self.__callbacks["connected"](client_info)
            except:
                pass

            self.__process_socket_data_in()
            self.__process_socket_data_out()

    def __read_from_one_client(self, client):
        byteData = client[0].recv(4096)
        retData = byteData
        while byteData and len(byteData):
            byteData = client[0].recv(4096)
            retData = retData + byteData

        if "message" in self.__callbacks:
            self.__callbacks["message"](client, retData)

    def __process_socket_data_in(self):
        for client in self.__clients:
            self.__read_from_one_client(client)

    def __process_socket_data_out(self):
        self.__dataLock.acquire()
        for data in self.__dataStore:
            for client in self.__clients:
                client[0].send(data)
        self.__dataStore = []
        self.__dataLock.release()

    def send(self, data):
        self.__dataLock.acquire()
        self.__dataStore.append(data)
        self.__dataLock.release()

# Main Server
class OpenCLGAServer():
    def __init__(self, options, ip="0.0.0.0", port=12345):
        self.__paused = False
        self.__forceStop = False
        self.__options = options
        self.__callbacks = {
            "connected": [],
            "disconnected": [],
            "message": []
        }
        self.__listen_at(ip, port)

    def __listen_at(self, ip, port):
        '''
        we should create a server socket and bind at all IP address with specified port.
        all commands are passed to client and wait for client's feedback.
        '''
        self.__server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__server_socket.bind((ip, port))

    def __send(self, command, data):
        pass

    def __process_data(self, data):
        pass

    def __notify(self, name, data):
        if name not in self.__callbacks:
            return

        for func in self.__callbacks[name]:
            try:
                func(data)
            except Exception as e:
                print("exception while execution %s callback"%(name))
                print(traceback.format_exc())

    # public APIs
    @property
    def paused(self):
        return self.__paused

    @property
    def elapsed_time(self):
        return self.__elapsed_time

    def start_server(self):
        self.__server_thread = ServerThread(self.__server_socket)
        self.__server_thread.start()

    def on(self, name, func):
        if name in self.__callbacks:
            self.__callbacks[name].append(func)

    def off(self, name, func):
        if (name in self.__callbacks):
            self.__callbacks[name].remove(func)

    def prepare(self):
        pass

    def run(self, prob_mutate, prob_crossover):
        pass

    def stop(self):
        self.__forceStop = True

    def pause(self):
        self.__paused = True

    def save(self, filename):
        raise RuntimeError("OpenCL Server doesn't support save or restore")

    def restore(self, filename):
        raise RuntimeError("OpenCL Server doesn't support save or restore")

    def get_statistics(self):
        # think a good way to deal with asymmetric statistics
        pass

    def get_the_best(self):
        pass
