#include <iostream>
#include <algorithm>
#include <vector>
#include <fstream>
#include <string>
#include <sstream>

#include <boost/filesystem/operations.hpp>
#include <boost/static_assert.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/tss.hpp>
#include <boost/signals2.hpp>

typedef std::vector<std::string> chunks_array;

// input file
std::ifstream file;
boost::mutex file_mutex;

// file chunks
chunks_array chunks;
boost::mutex chunks_mutex;

// thread specific counter for unique file naming
boost::thread_specific_ptr<uint32_t> counter;

// worker threads group
boost::thread_group tg;

// constants
namespace limits
{
    // set integer size
    const uint32_t int_size = sizeof(uint32_t);
    // set maximal memory usage to 256Mb
    const uint32_t available_memory = 256 * 1024 * 1024;
    // get number of available cores in the system (set to 1 if hardware_concurrency returns 0)
    const uint32_t available_cores = std::max<uint32_t>(boost::thread::hardware_concurrency(), 1);
    // calculate chunk size aligned to 4 bytes boundary
    const uint32_t chunk_size = ((available_memory / available_cores + int_size - 1) & ~(int_size - 1));
}

// generate unique temporary file name based on thread id and sequence number
std::string temp_name()
{
    std::ostringstream stream;
    stream << "chunk_" << boost::this_thread::get_id() << "." << ++*counter.get();
    return stream.str();
}

// merge two file chunks into one sorted file
std::string merge_chunks(const std::string& file_inp1, const std::string& file_inp2)
{
    // generate file name for merged chunk
    const std::string file_name = temp_name();

    // open chunks for reading and result file for writing
    std::ifstream input1(file_inp1.c_str(), std::ios::binary);
    std::ifstream input2(file_inp2.c_str(), std::ios::binary);
    std::ofstream output(file_name.c_str(), std::ios::binary | std::ios::trunc);

    uint32_t temp1, temp2, index = 0;

    // read both values
    input1.read(reinterpret_cast<char*>(&temp1), limits::int_size);
    input2.read(reinterpret_cast<char*>(&temp2), limits::int_size);

    // create dynamic buffer for output buffering
    boost::scoped_array<int> buffer(new int[limits::chunk_size]);

    while (!input1.eof() && !input2.eof())
    {
        if (temp1 < temp2)
        {
            buffer[index] = temp1;
            input1.read(reinterpret_cast<char*>(&temp1), limits::int_size);
        } else
        {
            buffer[index] = temp2;
            input2.read(reinterpret_cast<char*>(&temp2), limits::int_size);
        }
        if (++index * limits::int_size == limits::chunk_size)
        {
            output.write(reinterpret_cast<char*>(buffer.get()), limits::chunk_size);
            output.flush();
            index = 0;
        }
    };

    // flush remaining data in buffer
    if (index != 0)
    {
        output.write(reinterpret_cast<char*>(buffer.get()), index * limits::int_size);
        output.flush();
    }

    // select non-empty stream and push remaining temp variable to output
    std::ifstream* stream_ptr = 0;
    if (!input1.eof())
    {
        output.write(reinterpret_cast<char*>(&temp1), limits::int_size);
        stream_ptr = &input1;
    } else if (!input2.eof())
    {
        output.write(reinterpret_cast<char*>(&temp2), limits::int_size);
        stream_ptr = &input2;
    }

    // copy remaining stream contents to output
    if (stream_ptr)
    {
        for (;;)
        {
            boost::this_thread::interruption_point();
            stream_ptr->read(reinterpret_cast<char*>(buffer.get()), limits::chunk_size);
            if (stream_ptr->gcount() == 0)
                break;
            output.write(reinterpret_cast<char*>(buffer.get()), stream_ptr->gcount());
            output.flush();
        }
    }

    // close all files
    input1.close();
    input2.close();
    output.close();

    // delete merged chunks
    boost::filesystem::remove(boost::filesystem::path(file_inp1));
    boost::filesystem::remove(boost::filesystem::path(file_inp2));

    // return newly created file name
    return file_name;
}

// read next chunk from global file (blocks other threads from reading the file)
uint32_t next_chunk(uint32_t *chunk)
{
    // block other threads from reading the file
    boost::lock_guard<boost::mutex> lock(file_mutex);

    // load chunk from file and return number of loaded bytes
    file.read(reinterpret_cast<char*>(chunk), limits::chunk_size);
    return static_cast<uint32_t>(file.gcount());
}

// load, sort and store chunks from input file
bool load_chunks(chunks_array& curr)
{
    // create dynamic buffer for chunk
    boost::scoped_array<uint32_t> chunk(new uint32_t[limits::chunk_size]);

    bool result = false;

    // load next chunk from input file
    while (uint32_t size = next_chunk(chunk.get()))
    {
        boost::this_thread::interruption_point();

        // sort chunk in memory
        std::sort(chunk.get(), chunk.get() + (size / limits::int_size));

        // dump chunk from memory to file
        curr.push_back(temp_name());
        std::ofstream chunk_file(curr.back().c_str(), std::ios::binary | std::ios::trunc);
        chunk_file.write(reinterpret_cast<char*>(chunk.get()), size);
        chunk_file.close();
        result = true;
    }

    return result;
}

// merge pair of chunks from array
template <typename mutex>
bool merge_chunk_pair(chunks_array& curr, mutex& m)
{
    // lock chunks mutex (or dummy_mutex if function called on thread local variable)
    boost::unique_lock<mutex> lock(m);

    // if nothing to process just return false
    if (curr.size() < 2)
        return false;

    // get first chunk and remove it from array
    std::string chunk1 = curr.back();
    curr.pop_back();
    
    // get the second chunk and remove it from array
    std::string chunk2 = curr.back();
    curr.pop_back();

    // unlock mutex to enable concurrent execution of merge_chunks function
    lock.unlock();

    // merge chunk1 and chunk2 into result
    std::string result = merge_chunks(chunk1, chunk2);

    // lock mutex again to be able to push data to curr
    lock.lock();

    // push result to array
    curr.push_back(result);

    return curr.size() > 1;
}

// merge list of chunk files
std::string merge_chunk_list(chunks_array& curr)
{
    if (curr.empty())
        throw std::runtime_error("nothing to merge, chunk list is empty");

    // merge all chunks until there is more then one chunk to process
    boost::signals2::dummy_mutex dummy;
    while (merge_chunk_pair(curr, dummy));

    // return last chunk file name to the caller
    return *curr.begin();
}

// worker thread procedure
void worker_thread()
{
    // set initial value from thread specific counter
    counter.reset(new uint32_t(0));
    try
    {
        // load all chunks
        chunks_array curr;
        if (load_chunks(curr))
        {
            // merge them and get the file name
            std::string file_name = merge_chunk_list(curr);

            // add file name to global list of chunks
            boost::lock_guard<boost::mutex> lock(file_mutex);
            chunks.push_back(file_name);
        }
        while (merge_chunk_pair(chunks, chunks_mutex));
    } catch (std::exception& e)
    {
        std::cerr << "error occurred in thread " << boost::this_thread::get_id() << ": " << e.what() << std::endl;
        tg.interrupt_all();
    }
}

void sort_file(const std::string& file_name, const std::string& out_file)
{
    // open input file
    file.open(file_name.c_str(), std::ios::binary);
    if (!file.is_open())
        throw std::runtime_error("file doesn't exists");

    // create one thread less then available cores in the system (main thread is doing useful work as well)
    for (uint32_t i = 0; i < limits::available_cores; ++i)
    {
        // call worker_thread function in separate thread
        tg.create_thread(worker_thread);
    }

    // join all threads and wait until all are finished
    tg.join_all();

    if (!chunks.empty())
    {
        boost::filesystem::path old_path(*chunks.begin());
        boost::filesystem::path new_path(out_file);

        if (boost::filesystem::exists(new_path))
            boost::filesystem::remove(new_path);

        boost::filesystem::rename(old_path, new_path);
    }
};

int main(int argc, char **argv)
{
    std::time_t started_at = std::time(0);

    if (argc < 3)
    {
        std::cerr << "not enough arguments" << std::endl;
        return EXIT_FAILURE;
    }
    try
    {
        sort_file(argv[1], argv[2]);
    } catch (std::exception& e)
    {
        std::cerr << e.what() << std::endl;
    }
}
