package client

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/gogo/status"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/path"
	pb "github.com/textileio/textile/v2/api/bucketsd/pb"
	"github.com/textileio/textile/v2/buckets"
	"github.com/textileio/textile/v2/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	// chunkSize for add file requests.
	chunkSize = 1024 * 32
)

// Client provides the client api.
type Client struct {
	c    pb.APIServiceClient
	conn *grpc.ClientConn
}

// NewClient starts the client.
func NewClient(target string, opts ...grpc.DialOption) (*Client, error) {
	conn, err := grpc.Dial(target, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		c:    pb.NewAPIServiceClient(conn),
		conn: conn,
	}, nil
}

// Close closes the client's grpc connection and cancels any active requests.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Create initializes a new bucket.
// The bucket name is only meant to help identify a bucket in a UI and is not unique.
func (c *Client) Create(ctx context.Context, opts ...CreateOption) (*pb.CreateResponse, error) {
	args := &createOptions{}
	for _, opt := range opts {
		opt(args)
	}
	var strCid string
	if args.fromCid.Defined() {
		strCid = args.fromCid.String()
	}
	return c.c.Create(ctx, &pb.CreateRequest{
		Name:         args.name,
		Private:      args.private,
		BootstrapCid: strCid,
	})
}

// Root returns the bucket root.
func (c *Client) Root(ctx context.Context, key string) (*pb.RootResponse, error) {
	return c.c.Root(ctx, &pb.RootRequest{
		Key: key,
	})
}

// Links returns a list of bucket path URL links.
func (c *Client) Links(ctx context.Context, key, pth string) (*pb.LinksResponse, error) {
	return c.c.Links(ctx, &pb.LinksRequest{
		Key:  key,
		Path: filepath.ToSlash(pth),
	})
}

// List returns a list of all bucket roots.
func (c *Client) List(ctx context.Context) (*pb.ListResponse, error) {
	return c.c.List(ctx, &pb.ListRequest{})
}

// ListIpfsPath returns items at a particular path in a UnixFS path living in the IPFS network.
func (c *Client) ListIpfsPath(ctx context.Context, pth path.Path) (*pb.ListIpfsPathResponse, error) {
	return c.c.ListIpfsPath(ctx, &pb.ListIpfsPathRequest{Path: pth.String()})
}

// ListPath returns information about a bucket path.
func (c *Client) ListPath(ctx context.Context, key, pth string) (*pb.ListPathResponse, error) {
	return c.c.ListPath(ctx, &pb.ListPathRequest{
		Key:  key,
		Path: filepath.ToSlash(pth),
	})
}

// SetPath set a particular path to an existing IPFS UnixFS DAG.
func (c *Client) SetPath(ctx context.Context, key, pth string, remoteCid cid.Cid) (*pb.SetPathResponse, error) {
	return c.c.SetPath(ctx, &pb.SetPathRequest{
		Key:  key,
		Path: filepath.ToSlash(pth),
		Cid:  remoteCid.String(),
	})
}

// PushPathResult contains the result of a Push.
type PushPathResult struct {
	Path   string
	Cid    cid.Cid
	Size   int64
	Pinned int64
	Root   path.Resolved

	err error
}

// PushPathQueue handles PushPath input and output.
type PushPathQueue struct {
	// Current contains the current push result.
	Current PushPathResult

	inCh   chan pushPath
	outCh  chan PushPathResult
	done   bool
	closed bool
	wg     sync.WaitGroup

	size     int64
	complete int64
}

type pushPath struct {
	path string
	r    io.ReadCloser
}

// Push adds one or more files to the queue.
// pth is the location relative to the bucket root at which to insert the file, e.g., "/path/to/dog.jpg".
// name is the location of the file on the local filesystem, e.g., "/Users/clyde/Downloads/dog.jpg".
func (c *PushPathQueue) Push(pth, name string) error {
	if c.closed {
		return errors.New("push queue is closed")
	}

	f, err := os.Open(name)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}

	c.wg.Add(1)
	atomic.AddInt64(&c.size, info.Size())
	c.inCh <- pushPath{
		path: filepath.ToSlash(pth),
		r:    f,
	}
	return nil
}

// Size returns the queue size in bytes.
func (c *PushPathQueue) Size() int64 {
	return atomic.LoadInt64(&c.size)
}

// Complete returns the portion of the queue size that has been pushed.
func (c *PushPathQueue) Complete() int64 {
	return atomic.LoadInt64(&c.complete)
}

// Next blocks while the queue is open, returning true when a result is ready.
// Use Current to access the result.
func (c *PushPathQueue) Next() bool {
	select {
	case r, ok := <-c.outCh:
		if !ok {
			return false
		}
		c.Current = r
		c.wg.Done()
		return true
	}
}

// Err returns the current queue error.
// Call this method before checking the value of Current.
func (c *PushPathQueue) Err() error {
	return c.Current.err
}

// Close the queue. Subsequent calls to Push will result in an error.
// Next will continue to block until Close is called.
func (c *PushPathQueue) Close() {
	if c.closed {
		return
	}
	c.closed = true
	c.wg.Done() // finish queue opener
}

func (c *PushPathQueue) shutdown() {
	if c.done {
		return
	}
	c.done = true
	close(c.inCh)
	close(c.outCh)
}

// PushPath pushes a file to a bucket path.
// This will return the resolved path and the bucket's new root path.
func (c *Client) PushPath(ctx context.Context, key string, opts ...Option) (*PushPathQueue, error) {
	args := &options{}
	for _, opt := range opts {
		opt(args)
	}

	stream, err := c.c.PushPath(ctx)
	if err != nil {
		return nil, err
	}
	var xr string
	if args.root != nil {
		xr = args.root.String()
	}

	if err := stream.Send(&pb.PushPathRequest{
		Payload: &pb.PushPathRequest_Header_{
			Header: &pb.PushPathRequest_Header{
				Key:  key,
				Root: xr,
			},
		},
	}); err != nil {
		return nil, err
	}

	q := &PushPathQueue{
		inCh:  make(chan pushPath),
		outCh: make(chan PushPathResult),
	}

	q.wg.Add(1) // queue opener
	go func() {
		q.wg.Wait()
		q.shutdown()
	}()

	go func() {
		for {
			rep, err := stream.Recv()
			if err == io.EOF {
				return
			} else if err != nil {
				q.outCh <- PushPathResult{err: err}
				return
			}

			id, err := cid.Parse(rep.Cid)
			if err != nil {
				q.outCh <- PushPathResult{err: err}
				return
			}
			root, err := util.NewResolvedPath(rep.Root.Path)
			if err != nil {
				q.outCh <- PushPathResult{err: err}
				return
			}
			q.outCh <- PushPathResult{
				Path:   rep.Path,
				Cid:    id,
				Size:   rep.Size,
				Pinned: rep.Pinned,
				Root:   root,
			}
		}
	}()

	chunkCh := make(chan *pb.PushPathRequest_Chunk)
	go func() {
		for c := range chunkCh {
			if err := stream.Send(&pb.PushPathRequest{
				Payload: &pb.PushPathRequest_Chunk_{
					Chunk: c,
				},
			}); err == io.EOF {
				return // error is waiting to be received with stream.Recv above
			} else if err != nil {
				q.outCh <- PushPathResult{err: err}
			}
			atomic.AddInt64(&q.complete, int64(len(c.Data)))
			if args.progress != nil {
				args.progress <- q.complete
			}
		}
	}()

	go func() {
		defer func() {
			_ = stream.CloseSend()
			close(chunkCh)
		}()
		for {
			select {
			case p, ok := <-q.inCh:
				if !ok {
					return
				}

				go func(p pushPath) {
					buf := make([]byte, chunkSize)
					for {
						n, err := p.r.Read(buf)
						if n > 0 {
							data := make([]byte, n)
							copy(data, buf[:n])
							chunkCh <- &pb.PushPathRequest_Chunk{
								Path: p.path,
								Data: data,
							}
						} else if err == io.EOF {
							chunkCh <- &pb.PushPathRequest_Chunk{
								Path: p.path,
							}
							p.r.Close()
							return
						} else if err != nil {
							q.outCh <- PushPathResult{err: err}
							return
						}
					}
				}(p)
			}
		}
	}()

	return q, nil
}

// PullPath pulls the bucket path, writing it to writer if it's a file.
func (c *Client) PullPath(ctx context.Context, key, pth string, writer io.Writer, opts ...Option) error {
	args := &options{}
	for _, opt := range opts {
		opt(args)
	}

	pth = filepath.ToSlash(pth)
	stream, err := c.c.PullPath(ctx, &pb.PullPathRequest{
		Key:  key,
		Path: pth,
	})
	if err != nil {
		return err
	}

	var written int64
	for {
		rep, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		n, err := writer.Write(rep.Chunk)
		if err != nil {
			return err
		}
		written += int64(n)
		if args.progress != nil {
			args.progress <- written
		}
	}
	return nil
}

// PullIpfsPath pulls the path from a remote UnixFS dag, writing it to writer if it's a file.
func (c *Client) PullIpfsPath(ctx context.Context, pth path.Path, writer io.Writer, opts ...Option) error {
	args := &options{}
	for _, opt := range opts {
		opt(args)
	}

	stream, err := c.c.PullIpfsPath(ctx, &pb.PullIpfsPathRequest{
		Path: pth.String(),
	})
	if err != nil {
		return err
	}

	var written int64
	for {
		rep, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		n, err := writer.Write(rep.Chunk)
		if err != nil {
			return err
		}
		written += int64(n)
		if args.progress != nil {
			args.progress <- written
		}
	}
	return nil
}

// Remove removes an entire bucket.
// Files and directories will be unpinned.
func (c *Client) Remove(ctx context.Context, key string) error {
	_, err := c.c.Remove(ctx, &pb.RemoveRequest{
		Key: key,
	})
	return err
}

// RemovePath removes the file or directory at path.
// Files and directories will be unpinned.
func (c *Client) RemovePath(ctx context.Context, key, pth string, opts ...Option) (path.Resolved, error) {
	args := &options{}
	for _, opt := range opts {
		opt(args)
	}
	var xr string
	if args.root != nil {
		xr = args.root.String()
	}
	res, err := c.c.RemovePath(ctx, &pb.RemovePathRequest{
		Key:  key,
		Path: filepath.ToSlash(pth),
		Root: xr,
	})
	if err != nil {
		return nil, err
	}
	return util.NewResolvedPath(res.Root.Path)
}

// PushPathAccessRoles updates path access roles by merging the pushed roles with existing roles.
// roles is a map of string marshaled public keys to path roles. A non-nil error is returned
// if the map keys are not unmarshalable to public keys.
// To delete a role for a public key, set its value to buckets.None.
func (c *Client) PushPathAccessRoles(ctx context.Context, key, pth string, roles map[string]buckets.Role) error {
	pbroles, err := buckets.RolesToPb(roles)
	if err != nil {
		return err
	}
	_, err = c.c.PushPathAccessRoles(ctx, &pb.PushPathAccessRolesRequest{
		Key:   key,
		Path:  filepath.ToSlash(pth),
		Roles: pbroles,
	})
	return err
}

// PullPathAccessRoles returns access roles for a path.
func (c *Client) PullPathAccessRoles(ctx context.Context, key, pth string) (map[string]buckets.Role, error) {
	res, err := c.c.PullPathAccessRoles(ctx, &pb.PullPathAccessRolesRequest{
		Key:  key,
		Path: filepath.ToSlash(pth),
	})
	if err != nil {
		return nil, err
	}
	return buckets.RolesFromPb(res.Roles)
}

// DefaultArchiveConfig gets the default archive config for the specified Bucket.
func (c *Client) DefaultArchiveConfig(ctx context.Context, key string) (*pb.ArchiveConfig, error) {
	res, err := c.c.DefaultArchiveConfig(ctx, &pb.DefaultArchiveConfigRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return res.ArchiveConfig, nil
}

// SetDefaultArchiveConfig sets the default archive config for the specified Bucket.
func (c *Client) SetDefaultArchiveConfig(ctx context.Context, key string, config *pb.ArchiveConfig) error {
	req := &pb.SetDefaultArchiveConfigRequest{
		Key:           key,
		ArchiveConfig: config,
	}
	_, err := c.c.SetDefaultArchiveConfig(ctx, req)
	return err
}

// Archive creates a Filecoin bucket archive via Powergate.
func (c *Client) Archive(ctx context.Context, key string, opts ...ArchiveOption) error {
	req := &pb.ArchiveRequest{
		Key: key,
	}
	for _, opt := range opts {
		opt(req)
	}
	_, err := c.c.Archive(ctx, req)
	return err
}

// Archives returns information about current and historical archives.
func (c *Client) Archives(ctx context.Context, key string) (*pb.ArchivesResponse, error) {
	return c.c.Archives(ctx, &pb.ArchivesRequest{Key: key})
}

// ArchiveWatch watches status events from a Filecoin bucket archive.
func (c *Client) ArchiveWatch(ctx context.Context, key string, ch chan<- string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.c.ArchiveWatch(ctx, &pb.ArchiveWatchRequest{Key: key})
	if err != nil {
		return err
	}
	for {
		reply, err := stream.Recv()
		if err == io.EOF || status.Code(err) == codes.Canceled {
			break
		}
		if err != nil {
			return err
		}
		ch <- reply.Msg
	}
	return nil
}
