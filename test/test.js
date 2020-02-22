const fdSlicer = require('../');
const fs = require('fs');
const crypto = require('crypto');
const path = require('path');
const streamEqual = require('stream-equal');
const assert = require('assert');
const Pend = require('pend');
const StreamSink = require('streamsink');

const testBlobFile = path.join(__dirname, "test-blob.bin");
const testBlobFileSize = 20 * 1024 * 1024;
const testOutBlobFile = path.join(__dirname, "test-blob-out.bin");

describe("FdSlicer", () => {
  before(done => {
    const out = fs.createWriteStream(testBlobFile);
    for (let i = 0; i < testBlobFileSize / 1024; i += 1) {
      out.write(crypto.pseudoRandomBytes(1024));
    }
    out.end();
    out.on('close', done);
  });
  beforeEach(() => {
    try {
      fs.unlinkSync(testOutBlobFile);
    } catch (err) {
    }
  });
  after(() => {
    try {
      fs.unlinkSync(testBlobFile);
      fs.unlinkSync(testOutBlobFile);
    } catch (err) {
    }
  });
  it("reads a 20MB file (autoClose on)", done => {
    fs.open(testBlobFile, 'r', (err, fd) => {
      if (err) return done(err);
      const slicer = fdSlicer.createFromFd(fd, {autoClose: true});
      const actualStream = slicer.createReadStream();
      const expectedStream = fs.createReadStream(testBlobFile);

      const pend = new Pend();
      pend.go(cb => {
        slicer.on('close', cb);
      });
      pend.go(cb => {
        streamEqual(expectedStream, actualStream, (err, equal) => {
          if (err) return done(err);
          assert.ok(equal);
          cb();
        });
      });
      pend.wait(done);
    });
  });
  it("reads 4 chunks simultaneously", done => {
    fs.open(testBlobFile, 'r', (err, fd) => {
      if (err) return done(err);
      const slicer = fdSlicer.createFromFd(fd);
      const actualPart1 = slicer.createReadStream({start: testBlobFileSize * 0/4, end: testBlobFileSize * 1/4});
      const actualPart2 = slicer.createReadStream({start: testBlobFileSize * 1/4, end: testBlobFileSize * 2/4});
      const actualPart3 = slicer.createReadStream({start: testBlobFileSize * 2/4, end: testBlobFileSize * 3/4});
      const actualPart4 = slicer.createReadStream({start: testBlobFileSize * 3/4, end: testBlobFileSize * 4/4});
      const expectedPart1 = slicer.createReadStream({start: testBlobFileSize * 0/4, end: testBlobFileSize * 1/4});
      const expectedPart2 = slicer.createReadStream({start: testBlobFileSize * 1/4, end: testBlobFileSize * 2/4});
      const expectedPart3 = slicer.createReadStream({start: testBlobFileSize * 2/4, end: testBlobFileSize * 3/4});
      const expectedPart4 = slicer.createReadStream({start: testBlobFileSize * 3/4, end: testBlobFileSize * 4/4});
      const pend = new Pend();
      pend.go(cb => {
        streamEqual(expectedPart1, actualPart1, (err, equal) => {
          assert.ok(equal);
          cb(err);
        });
      });
      pend.go(cb => {
        streamEqual(expectedPart2, actualPart2, (err, equal) => {
          assert.ok(equal);
          cb(err);
        });
      });
      pend.go(cb => {
        streamEqual(expectedPart3, actualPart3, (err, equal) => {
          assert.ok(equal);
          cb(err);
        });
      });
      pend.go(cb => {
        streamEqual(expectedPart4, actualPart4, (err, equal) => {
          assert.ok(equal);
          cb(err);
        });
      });
      pend.wait(err => {
        if (err) return done(err);
        fs.close(fd, done);
      });
    });
  });

  it("writes a 20MB file (autoClose on)", done => {
    fs.open(testOutBlobFile, 'w', (err, fd) => {
      if (err) return done(err);
      const slicer = fdSlicer.createFromFd(fd, {autoClose: true});
      const actualStream = slicer.createWriteStream();
      const inStream = fs.createReadStream(testBlobFile);

      slicer.on('close', () => {
        const expected = fs.createReadStream(testBlobFile);
        const actual = fs.createReadStream(testOutBlobFile);

        streamEqual(expected, actual, (err, equal) => {
          if (err) return done(err);
          assert.ok(equal);
          done();
        });
      });
      inStream.pipe(actualStream);
    });
  });

  it("writes 4 chunks simultaneously", done => {
    fs.open(testOutBlobFile, 'w', (err, fd) => {
      if (err) return done(err);
      const slicer = fdSlicer.createFromFd(fd);
      const actualPart1 = slicer.createWriteStream({start: testBlobFileSize * 0/4});
      const actualPart2 = slicer.createWriteStream({start: testBlobFileSize * 1/4});
      const actualPart3 = slicer.createWriteStream({start: testBlobFileSize * 2/4});
      const actualPart4 = slicer.createWriteStream({start: testBlobFileSize * 3/4});
      const in1 = fs.createReadStream(testBlobFile, {start: testBlobFileSize * 0/4, end: testBlobFileSize * 1/4});
      const in2 = fs.createReadStream(testBlobFile, {start: testBlobFileSize * 1/4, end: testBlobFileSize * 2/4});
      const in3 = fs.createReadStream(testBlobFile, {start: testBlobFileSize * 2/4, end: testBlobFileSize * 3/4});
      const in4 = fs.createReadStream(testBlobFile, {start: testBlobFileSize * 3/4, end: testBlobFileSize * 4/4});
      const pend = new Pend();
      pend.go(cb => {
        actualPart1.on('finish', cb);
      });
      pend.go(cb => {
        actualPart2.on('finish', cb);
      });
      pend.go(cb => {
        actualPart3.on('finish', cb);
      });
      pend.go(cb => {
        actualPart4.on('finish', cb);
      });
      in1.pipe(actualPart1);
      in2.pipe(actualPart2);
      in3.pipe(actualPart3);
      in4.pipe(actualPart4);
      pend.wait(() => {
        fs.close(fd, err => {
          if (err) return done(err);
          const expected = fs.createReadStream(testBlobFile);
          const actual = fs.createReadStream(testOutBlobFile);
          streamEqual(expected, actual, (err, equal) => {
            if (err) return done(err);
            assert.ok(equal);
            done();
          });
        });
      });
    });
  });

  it("throws on invalid ref", done => {
    fs.open(testOutBlobFile, 'w', (err, fd) => {
      if (err) return done(err);
      const slicer = fdSlicer.createFromFd(fd, {autoClose: true});
      assert.throws(() => {
        slicer.unref();
      }, /invalid unref/);
      fs.close(fd, done);
    });
  });

  it("write stream emits error when max size exceeded", done => {
    fs.open(testOutBlobFile, 'w', (err, fd) => {
      if (err) return done(err);
      const slicer = fdSlicer.createFromFd(fd, {autoClose: true});
      const ws = slicer.createWriteStream({start: 0, end: 1000});
      ws.on('error', ({code}) => {
        assert.strictEqual(code, 'ETOOBIG');
        slicer.on('close', done);
      });
      ws.end(Buffer.alloc(1001));
    });
  });

  it("write stream does not emit error when max size not exceeded", done => {
    fs.open(testOutBlobFile, 'w', (err, fd) => {
      if (err) return done(err);
      const slicer = fdSlicer.createFromFd(fd, {autoClose: true});
      const ws = slicer.createWriteStream({end: 1000});
      slicer.on('close', done);
      ws.end(Buffer.alloc(1000));
    });
  });

  it("write stream start and end work together", done => {
    fs.open(testOutBlobFile, 'w', (err, fd) => {
      if (err) return done(err);
      const slicer = fdSlicer.createFromFd(fd, {autoClose: true});
      const ws = slicer.createWriteStream({start: 1, end: 1000});
      ws.on('error', ({code}) => {
        assert.strictEqual(code, 'ETOOBIG');
        slicer.on('close', done);
      });
      ws.end(Buffer.alloc(1000));
    });
  });

  it("write stream emits progress events", done => {
    fs.open(testOutBlobFile, 'w', (err, fd) => {
      if (err) return done(err);
      const slicer = fdSlicer.createFromFd(fd, {autoClose: true});
      const ws = slicer.createWriteStream();
      let progressEventCount = 0;
      let prevBytesWritten = 0;
      ws.on('progress', () => {
        progressEventCount += 1;
        assert.ok(ws.bytesWritten > prevBytesWritten);
        prevBytesWritten = ws.bytesWritten;
      });
      slicer.on('close', () => {
        assert.ok(progressEventCount > 5);
        done();
      });
      for (let i = 0; i < 10; i += 1) {
        ws.write(Buffer.alloc(16 * 1024 * 2));
      }
      ws.end();
    });
  });

  it("write stream unrefs when destroyed", done => {
    fs.open(testOutBlobFile, 'w', (err, fd) => {
      if (err) return done(err);
      const slicer = fdSlicer.createFromFd(fd, {autoClose: true});
      const ws = slicer.createWriteStream();
      slicer.on('close', done);
      ws.write(Buffer.alloc(1000));
      ws.destroy();
    });
  });

  it("read stream unrefs when destroyed", done => {
    fs.open(testBlobFile, 'r', (err, fd) => {
      if (err) return done(err);
      const slicer = fdSlicer.createFromFd(fd, {autoClose: true});
      const rs = slicer.createReadStream();
      rs.on('error', ({message}) => {
        assert.strictEqual(message, "stream destroyed");
        slicer.on('close', done);
      });
      rs.destroy();
    });
  });

  it("fdSlicer.read", done => {
    fs.open(testBlobFile, 'r', (err, fd) => {
      if (err) return done(err);
      const slicer = fdSlicer.createFromFd(fd);
      const outBuf = Buffer.alloc(1024);
      slicer.read(outBuf, 0, 10, 0, (err, bytesRead, buf) => {
        assert.strictEqual(bytesRead, 10);
        fs.close(fd, done);
      });
    });
  });

  it("fdSlicer.write", done => {
    fs.open(testOutBlobFile, 'w', (err, fd) => {
      if (err) return done(err);
      const slicer = fdSlicer.createFromFd(fd);
      slicer.write(Buffer.from("blah\n"), 0, 5, 0, () => {
        if (err) return done(err);
        fs.close(fd, done);
      });
    });
  });
});

describe("BufferSlicer", () => {
  it("invalid ref", () => {
    const slicer = fdSlicer.createFromBuffer(Buffer.alloc(16));
    slicer.ref();
    slicer.unref();
    assert.throws(() => {
      slicer.unref();
    }, /invalid unref/);
  });
  it("read and write", done => {
    const buf = Buffer.from("through the tangled thread the needle finds its way");
    const slicer = fdSlicer.createFromBuffer(buf);
    const outBuf = Buffer.alloc(1024);
    slicer.read(outBuf, 10, 11, 8, err => {
      if (err) return done(err);
      assert.strictEqual(outBuf.toString('utf8', 10, 21), "the tangled");
      slicer.write(Buffer.from("derp"), 0, 4, 7, err => {
        if (err) return done(err);
        assert.strictEqual(buf.toString('utf8', 7, 19), "derp tangled");
        done();
      });
    });
  });
  it("createReadStream", done => {
    const str = "I never conquered rarely came, 16 just held such better days";
    const buf = Buffer.from(str);
    const slicer = fdSlicer.createFromBuffer(buf);
    const inStream = slicer.createReadStream();
    const sink = new StreamSink();
    inStream.pipe(sink);
    sink.on('finish', () => {
      assert.strictEqual(sink.toString(), str);
      inStream.destroy();
      done();
    });
  });
  it("createWriteStream exceed buffer size", done => {
    const slicer = fdSlicer.createFromBuffer(Buffer.alloc(4));
    const outStream = slicer.createWriteStream();
    outStream.on('error', ({code}) => {
      assert.strictEqual(code, 'ETOOBIG');
      done();
    });
    outStream.write("hi!\n");
    outStream.write("it warked\n");
    outStream.end();
  });
  it("createWriteStream ok", done => {
    const buf = Buffer.alloc(1024);
    const slicer = fdSlicer.createFromBuffer(buf);
    const outStream = slicer.createWriteStream();
    outStream.on('finish', () => {
      assert.strictEqual(buf.toString('utf8', 0, "hi!\nit warked\n".length), "hi!\nit warked\n");
      outStream.destroy();
      done();
    });
    outStream.write("hi!\n");
    outStream.write("it warked\n");
    outStream.end();
  });
});
