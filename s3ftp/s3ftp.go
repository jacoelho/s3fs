package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"path"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/jacoelho/s3fs"
)

// Based on example server code from golang.org/x/crypto/ssh and server_standalone
func main() {
	var (
		readOnly    bool
		debugStderr bool
	)

	flag.BoolVar(&readOnly, "R", false, "read-only server")
	flag.BoolVar(&debugStderr, "e", false, "debug to stderr")
	flag.Parse()

	debugStream := io.Discard
	if debugStderr {
		debugStream = os.Stderr
	}

	// An SSH server is represented by a ServerConfig, which holds
	// certificate details and handles authentication of ServerConns.
	config := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			// Should use constant-time compare (or better, salt+hash) in
			// a production setting.
			fmt.Fprintf(debugStream, "Login: %s\n", c.User())
			if c.User() == "testuser" && string(pass) == "tiger" {
				return nil, nil
			}
			return nil, fmt.Errorf("password rejected for %q", c.User())
		},
	}

	privateBytes, err := os.ReadFile("id_rsa")
	if err != nil {
		log.Fatal("Failed to load private key", err)
	}

	private, err := ssh.ParsePrivateKey(privateBytes)
	if err != nil {
		log.Fatal("Failed to parse private key", err)
	}

	config.AddHostKey(private)

	// Once a ServerConfig has been configured, connections can be
	// accepted.
	listener, err := net.Listen("tcp", "0.0.0.0:2022")
	if err != nil {
		log.Fatal("failed to listen for connection", err)
	}
	fmt.Printf("Listening on %v\n", listener.Addr())

	nConn, err := listener.Accept()
	if err != nil {
		log.Fatal("failed to accept incoming connection", err)
	}

	// Before use, a handshake must be performed on the incoming
	// net.Conn.
	_, chans, reqs, err := ssh.NewServerConn(nConn, config)
	if err != nil {
		log.Fatal("failed to handshake", err)
	}
	fmt.Fprintf(debugStream, "SSH server established\n")

	// The incoming Request channel must be serviced.
	go ssh.DiscardRequests(reqs)

	// Service the incoming Channel channel.
	for newChannel := range chans {
		// Channels have a type, depending on the application level
		// protocol intended. In the case of an SFTP session, this is "subsystem"
		// with a payload string of "<length=4>sftp"
		fmt.Fprintf(debugStream, "Incoming channel: %s\n", newChannel.ChannelType())
		if newChannel.ChannelType() != "session" {
			newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
			fmt.Fprintf(debugStream, "Unknown channel type: %s\n", newChannel.ChannelType())
			continue
		}
		channel, requests, err := newChannel.Accept()
		if err != nil {
			log.Fatal("could not accept channel.", err)
		}
		fmt.Fprintf(debugStream, "Channel accepted\n")

		// Sessions have out-of-band requests such as "shell",
		// "pty-req" and "env".  Here we handle only the
		// "subsystem" request.
		go func(in <-chan *ssh.Request) {
			for req := range in {
				fmt.Fprintf(debugStream, "Request: %v\n", req.Type)
				ok := false
				switch req.Type {
				case "subsystem":
					fmt.Fprintf(debugStream, "Subsystem: %s\n", req.Payload[4:])
					if string(req.Payload[4:]) == "sftp" {
						ok = true
					}
				}
				fmt.Fprintf(debugStream, " - accepted: %v\n", ok)
				req.Reply(ok, nil)

				root := sftpHandler(&foo{})
				sftpServer := sftp.NewRequestServer(channel, root)
				if err := sftpServer.Serve(); err != nil && err != io.EOF {
					log.Println(err)
					return
				}
			}
		}(requests)

		serverOptions := []sftp.ServerOption{
			sftp.WithDebug(debugStream),
		}

		if readOnly {
			serverOptions = append(serverOptions, sftp.ReadOnly())
			fmt.Fprintf(debugStream, "Read-only server\n")
		} else {
			fmt.Fprintf(debugStream, "Read write server\n")
		}

		server, err := sftp.NewServer(
			channel,
			serverOptions...,
		)
		if err != nil {
			log.Fatal(err)
		}
		if err := server.Serve(); err == io.EOF {
			server.Close()
			log.Print("sftp client exited session.")
		} else if err != nil {
			log.Fatal("sftp server completed with error:", err)
		}
	}
}

// https://github.com/lambdaxymox/fuchsia/blob/a3fa47a49d96eec82c74c8fc9217f971609dc640/tools/fuzz/ssh_fake.go

func sftpHandler(handler *foo) sftp.Handlers {
	return sftp.Handlers{
		FileGet:  handler,
		FilePut:  handler,
		FileCmd:  handler,
		FileList: handler,
	}
}

type foo struct {
	fs *s3fs.Fs
}

func (f foo) Filelist(request *sftp.Request) (sftp.ListerAt, error) {
	file, err := f.fs.Open(request.Filepath)
	if err != nil {
		return nil, err
	}

	info, _ := file.Stat()

	switch request.Method {
	case "Stat":
		if err != nil {
			return nil, err
		}
		return listerat{info}, nil

	case "List":
		if !info.IsDir() {
			return listerat{info}, nil
		}
		_ = file.Close()
		entries, err := fs.ReadDir(f.fs, request.Filepath)
		if err != nil {
			return nil, err
		}

		lst := make(listerat, len(entries))
		for i := range entries {
			info, _ := entries[i].Info()
			lst[i] = info
		}
		return lst, nil
	default:
		return nil, fmt.Errorf("not support: %v", request.Method)
	}
}

func (f foo) Filecmd(request *sftp.Request) error {
	switch request.Method {
	case "Mkdir":
		file, err := f.fs.Create(path.Join(request.Filepath, ".keep"))
		if err != nil {
			return err
		}
		if _, err := io.WriteString(file, "placeholder"); err != nil {
			return err
		}

		return file.Close()
	default:
		return fmt.Errorf("not support: %v", request.Method)
	}
}

func (f foo) Filewrite(request *sftp.Request) (io.WriterAt, error) {
	file, err := f.fs.Create(request.Filepath)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (f foo) Fileread(request *sftp.Request) (io.ReaderAt, error) {
	file, err := f.fs.Open(request.Filepath)
	if err != nil {
		return nil, err
	}

	fileReader, ok := file.(*s3fs.File)
	if !ok {
		return nil, errors.New("not expected")
	}

	return fileReader, nil
}

type listerat []os.FileInfo

func (f listerat) ListAt(ls []os.FileInfo, offset int64) (int, error) {
	var n int
	if offset >= int64(len(f)) {
		return 0, io.EOF
	}
	n = copy(ls, f[offset:])
	if n < len(ls) {
		return n, io.EOF
	}
	return n, nil
}
