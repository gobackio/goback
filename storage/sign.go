package storage

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"

	gcs "cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/blob"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var _ pack.RangeSigner = (*BucketStore)(nil)

// rangeHeader is the header a signed range is bound to, so a URL cannot
// be turned on the rest of the archive.
const rangeHeader = "Range"

// SignedRange asks for a GET of bytes [Offset, Offset+Length) of the
// archive Name, working for TTL from Now (zero means time.Now). Header
// is bound into the signature beside the range, and the request must
// send it as given.
type SignedRange struct {
	Name           string
	Offset, Length int64
	TTL            time.Duration
	Now            time.Time
	Header         map[string]string
}

// SignArchiveRange signs r against bucket, which holds archives as a
// BucketStore keeps them. A bucket whose driver cannot bind the range and
// headers answers pack.ErrNoSignedURL; only Cloud Storage binds headers
// beyond the range.
func SignArchiveRange(ctx context.Context, bucket *blob.Bucket, r SignedRange) (*proto.Location, error) {
	if r.Length <= 0 {
		return nil, fmt.Errorf("%w: empty range of %s", pack.ErrNoSignedURL, r.Name)
	}

	now := r.Now
	if now.IsZero() {
		now = time.Now()
	}

	value := fmt.Sprintf("bytes=%d-%d", r.Offset, r.Offset+r.Length-1)

	bound := false
	signed, err := bucket.SignedURL(ctx, ArchiveKey(r.Name), &blob.SignedURLOptions{
		Expiry: r.TTL,
		Method: "GET",
		BeforeSign: func(as func(any) bool) error {
			bound = bind(as, value, r.Header)

			return nil
		},
	})

	if err != nil {
		return nil, fmt.Errorf("%w: %v", pack.ErrNoSignedURL, err)
	}

	header := map[string]string{rangeHeader: value}
	for name, v := range r.Header {
		header[name] = v
	}

	if !bound || !signsHeaders(signed, header) {
		return nil, fmt.Errorf("%w: this bucket cannot bind the range and its headers", pack.ErrNoSignedURL)
	}

	return &proto.Location{
		Url:     signed,
		Header:  header,
		Length:  r.Length,
		Expires: timestamppb.New(now.Add(r.TTL)),
	}, nil
}

// bind puts the range and the extra headers among those the signature
// covers, for each driver that can, and reports whether one did.
func bind(as func(any) bool, value string, extra map[string]string) bool {
	var gcsOptions *gcs.SignedURLOptions
	if as(&gcsOptions) {
		// V2, the library's default, signs no Range header
		gcsOptions.Scheme = gcs.SigningSchemeV4
		gcsOptions.Headers = append(gcsOptions.Headers, rangeHeader+":"+value)
		for name, v := range extra {
			gcsOptions.Headers = append(gcsOptions.Headers, name+":"+v)
		}

		return true
	}

	var s3Get *s3.GetObjectInput
	if len(extra) == 0 && as(&s3Get) {
		s3Get.Range = aws.String(value)

		return true
	}

	return false
}

// signsHeaders reports whether the V4 signature of signed, on Cloud
// Storage or S3, covers every header in header.
func signsHeaders(signed string, header map[string]string) bool {
	u, err := url.Parse(signed)
	if err != nil {
		return false
	}

	query := u.Query()
	names := query.Get("X-Goog-SignedHeaders")
	if names == "" {
		names = query.Get("X-Amz-SignedHeaders")
	}

	covered := map[string]bool{}
	for _, name := range strings.Split(names, ";") {
		covered[strings.ToLower(name)] = true
	}

	for name := range header {
		if !covered[strings.ToLower(name)] {
			return false
		}
	}

	return true
}

// SignRange implements pack.RangeSigner. The URL is bound to the range,
// so the recipient can read those bytes and nothing else of the archive.
// A bucket whose driver cannot bind a header answers ErrNoSignedURL.
func (c *BucketStore) SignRange(ctx context.Context, name string, offset, length int64, ttl time.Duration) (*proto.Location, error) {
	return SignArchiveRange(ctx, c.bucket, SignedRange{Name: name, Offset: offset, Length: length, TTL: ttl})
}
