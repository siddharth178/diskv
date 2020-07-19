I am learning rust and this is an attempt to port [peterbourgon's diskv](https://github.com/peterbourgon/diskv) to rust, as much as possible.

### TODO
2. Use Reader/Writer kind of pattern
3. Custom path transformations
4. Compression
5. Benchmarks
6. More about locks (read lock released in try_read) and its effects
    a. in `Diskv::get`, there is a possibility of someone deleting the key when we are reading the file and before we Write-lock it in `put`.
    b. can key based locked help in throughput?
