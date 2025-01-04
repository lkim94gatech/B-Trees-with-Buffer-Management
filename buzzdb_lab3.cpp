#include <iostream>
#include <algorithm>
#include <fstream>
#include <chrono>
#include <map>
#include <vector>
#include <list>
#include <unordered_map>
#include <string>
#include <memory>
#include <sstream>
#include <limits>
#include <thread>
#include <queue>
#include <optional>
#include <random>
#include <mutex>
#include <shared_mutex>
#include <cassert>
#include <cstring> 
#include <exception>
#include <atomic>
#include <set>

#define UNUSED(p)  ((void)(p))

#define ASSERT_WITH_MESSAGE(condition, message) \
    do { \
        if (!(condition)) { \
            std::cerr << "Assertion \033[1;31mFAILED\033[0m: " << message << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
            std::abort(); \
        } \
    } while(0)

enum FieldType { INT, FLOAT, STRING };

// Define a basic Field variant class that can hold different types
class Field {
public:
    FieldType type;
    std::unique_ptr<char[]> data;
    size_t data_length;

public:
    Field(int i) : type(INT) { 
        data_length = sizeof(int);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &i, data_length);
    }

    Field(float f) : type(FLOAT) { 
        data_length = sizeof(float);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &f, data_length);
    }

    Field(const std::string& s) : type(STRING) {
        data_length = s.size() + 1;  // include null-terminator
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), s.c_str(), data_length);
    }

    Field& operator=(const Field& other) {
        if (&other == this) {
            return *this;
        }
        type = other.type;
        data_length = other.data_length;
        std::memcpy(data.get(), other.data.get(), data_length);
        return *this;
    }

    Field(Field&& other){
        type = other.type;
        data_length = other.data_length;
        std::memcpy(data.get(), other.data.get(), data_length);
    }

    FieldType getType() const { return type; }
    int asInt() const { 
        return *reinterpret_cast<int*>(data.get());
    }
    float asFloat() const { 
        return *reinterpret_cast<float*>(data.get());
    }
    std::string asString() const { 
        return std::string(data.get());
    }

    std::string serialize() {
        std::stringstream buffer;
        buffer << type << ' ' << data_length << ' ';
        if (type == STRING) {
            buffer << data.get() << ' ';
        } else if (type == INT) {
            buffer << *reinterpret_cast<int*>(data.get()) << ' ';
        } else if (type == FLOAT) {
            buffer << *reinterpret_cast<float*>(data.get()) << ' ';
        }
        return buffer.str();
    }

    void serialize(std::ofstream& out) {
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    static std::unique_ptr<Field> deserialize(std::istream& in) {
        int type; in >> type;
        size_t length; in >> length;
        if (type == STRING) {
            std::string val; in >> val;
            return std::make_unique<Field>(val);
        } else if (type == INT) {
            int val; in >> val;
            return std::make_unique<Field>(val);
        } else if (type == FLOAT) {
            float val; in >> val;
            return std::make_unique<Field>(val);
        }
        return nullptr;
    }

    void print() const{
        switch(getType()){
            case INT: std::cout << asInt(); break;
            case FLOAT: std::cout << asFloat(); break;
            case STRING: std::cout << asString(); break;
        }
    }
};

class Tuple {
public:
    std::vector<std::unique_ptr<Field>> fields;

    void addField(std::unique_ptr<Field> field) {
        fields.push_back(std::move(field));
    }

    size_t getSize() const {
        size_t size = 0;
        for (const auto& field : fields) {
            size += field->data_length;
        }
        return size;
    }

    std::string serialize() {
        std::stringstream buffer;
        buffer << fields.size() << ' ';
        for (const auto& field : fields) {
            buffer << field->serialize();
        }
        return buffer.str();
    }

    void serialize(std::ofstream& out) {
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    static std::unique_ptr<Tuple> deserialize(std::istream& in) {
        auto tuple = std::make_unique<Tuple>();
        size_t fieldCount; in >> fieldCount;
        for (size_t i = 0; i < fieldCount; ++i) {
            tuple->addField(Field::deserialize(in));
        }
        return tuple;
    }

    void print() const {
        for (const auto& field : fields) {
            field->print();
            std::cout << " ";
        }
        std::cout << "\n";
    }
};

static constexpr size_t PAGE_SIZE = 4096;  // Fixed page size
static constexpr size_t MAX_SLOTS = 512;   // Fixed number of slots
static constexpr size_t MAX_PAGES= 1000;   // Total Number of pages that can be stored
uint16_t INVALID_VALUE = std::numeric_limits<uint16_t>::max(); // Sentinel value

struct Slot {
    bool empty = true;                 // Is the slot empty?    
    uint16_t offset = INVALID_VALUE;    // Offset of the slot within the page
    uint16_t length = INVALID_VALUE;    // Length of the slot
};

// Slotted Page class
class SlottedPage {
public:
    std::unique_ptr<char[]> page_data = std::make_unique<char[]>(PAGE_SIZE);
    size_t metadata_size = sizeof(Slot) * MAX_SLOTS;

    SlottedPage(){
        // Empty page -> initialize slot array inside page
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            slot_array[slot_itr].empty = true;
            slot_array[slot_itr].offset = INVALID_VALUE;
            slot_array[slot_itr].length = INVALID_VALUE;
        }
    }

    // Add a tuple, returns true if it fits, false otherwise.
    bool addTuple(std::unique_ptr<Tuple> tuple) {

        // Serialize the tuple into a char array
        auto serializedTuple = tuple->serialize();
        size_t tuple_size = serializedTuple.size();

        //std::cout << "Tuple size: " << tuple_size << " bytes\n";
        assert(tuple_size == 38);

        // Check for first slot with enough space
        size_t slot_itr = 0;
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());        
        for (; slot_itr < MAX_SLOTS; slot_itr++) {
            if (slot_array[slot_itr].empty == true and 
                slot_array[slot_itr].length >= tuple_size) {
                break;
            }
        }
        if (slot_itr == MAX_SLOTS){
            //std::cout << "Page does not contain an empty slot with sufficient space to store the tuple.";
            return false;
        }

        // Identify the offset where the tuple will be placed in the page
        // Update slot meta-data if needed
        slot_array[slot_itr].empty = false;
        size_t offset = INVALID_VALUE;
        if (slot_array[slot_itr].offset == INVALID_VALUE){
            if(slot_itr != 0){
                auto prev_slot_offset = slot_array[slot_itr - 1].offset;
                auto prev_slot_length = slot_array[slot_itr - 1].length;
                offset = prev_slot_offset + prev_slot_length;
            }
            else{
                offset = metadata_size;
            }

            slot_array[slot_itr].offset = offset;
        }
        else{
            offset = slot_array[slot_itr].offset;
        }

        if(offset + tuple_size >= PAGE_SIZE){
            slot_array[slot_itr].empty = true;
            slot_array[slot_itr].offset = INVALID_VALUE;
            return false;
        }

        assert(offset != INVALID_VALUE);
        assert(offset >= metadata_size);
        assert(offset + tuple_size < PAGE_SIZE);

        if (slot_array[slot_itr].length == INVALID_VALUE){
            slot_array[slot_itr].length = tuple_size;
        }

        // Copy serialized data into the page
        std::memcpy(page_data.get() + offset, 
                    serializedTuple.c_str(), 
                    tuple_size);

        return true;
    }

    void deleteTuple(size_t index) {
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        size_t slot_itr = 0;
        for (; slot_itr < MAX_SLOTS; slot_itr++) {
            if(slot_itr == index and
               slot_array[slot_itr].empty == false){
                slot_array[slot_itr].empty = true;
                break;
               }
        }

        //std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void print() const{
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            if (slot_array[slot_itr].empty == false){
                assert(slot_array[slot_itr].offset != INVALID_VALUE);
                const char* tuple_data = page_data.get() + slot_array[slot_itr].offset;
                std::istringstream iss(tuple_data);
                auto loadedTuple = Tuple::deserialize(iss);
                std::cout << "Slot " << slot_itr << " : [";
                std::cout << (uint16_t)(slot_array[slot_itr].offset) << "] :: ";
                loadedTuple->print();
            }
        }
        std::cout << "\n";
    }
};

const std::string database_filename = "buzzdb.dat";

class StorageManager {
public:    
    std::fstream fileStream;
    size_t num_pages = 0;
    std::mutex io_mutex;

public:
    StorageManager(bool truncate_mode = true){
        auto flags = truncate_mode ? std::ios::in | std::ios::out | std::ios::trunc 
        : std::ios::in | std::ios::out;
        fileStream.open(database_filename, flags);
        if (!fileStream) {
            fileStream.clear();
            fileStream.open(database_filename, truncate_mode ? (std::ios::out | std::ios::trunc) : std::ios::out);
        }
        fileStream.close();
        fileStream.open(database_filename, std::ios::in | std::ios::out);

        // Initialize entire file with empty pages
        fileStream.seekg(0, std::ios::end);
        num_pages = fileStream.tellg() / PAGE_SIZE;
        
        if (num_pages < MAX_PAGES) {
            // Pre-allocate all pages
            char* empty_buffer = new char[PAGE_SIZE]();
            fileStream.seekp(0, std::ios::end);
            for (size_t i = num_pages; i < MAX_PAGES; i++) {
                fileStream.write(empty_buffer, PAGE_SIZE);
            }
            fileStream.flush();
            delete[] empty_buffer;
            num_pages = MAX_PAGES;
        }

    }

    ~StorageManager() {
        if (fileStream.is_open()) {
            fileStream.close();
        }
    }

    // Read a page from disk
    std::unique_ptr<SlottedPage> load(uint16_t page_id) {
        fileStream.seekg(page_id * PAGE_SIZE, std::ios::beg);
        auto page = std::make_unique<SlottedPage>();
        // Read the content of the file into the page
        if(fileStream.read(page->page_data.get(), PAGE_SIZE)){
            //std::cout << "Page read successfully from file." << std::endl;
        }
        else{
            std::cerr << "Error: Unable to read data from the file. \n";
            exit(-1);
        }
        return page;
    }

    // Write a page to disk
    void flush(uint16_t page_id, const SlottedPage& page) {
        size_t page_offset = page_id * PAGE_SIZE;        

        // Move the write pointer
        fileStream.seekp(page_offset, std::ios::beg);
        fileStream.write(page.page_data.get(), PAGE_SIZE);        
        fileStream.flush();
    }

    // Extend database file by one page
    void extend() {
        // Create a slotted page
        auto empty_slotted_page = std::make_unique<SlottedPage>();

        // Make sure stream is open
        if (!fileStream.is_open()) {
            fileStream.clear();
            fileStream.open(database_filename, std::ios::in | std::ios::out);
        }

        // Move the write pointer
        fileStream.seekp(0, std::ios::end);

        // Write the page to the file, extending it
        if (!fileStream.write(empty_slotted_page->page_data.get(), PAGE_SIZE)) {
            std::cerr << "Failed to write to file during extend" << std::endl;
            return;
        }
        fileStream.flush();

        // Update number of pages
        num_pages += 1;
    }

    void extend(uint64_t till_page_id) {
        std::lock_guard<std::mutex>  io_guard(io_mutex); 
        uint64_t write_size = std::max(static_cast<uint64_t>(0), till_page_id + 1 - num_pages) * PAGE_SIZE;
        if(write_size > 0 ) {
            // std::cout << "Extending database file till page id : "<<till_page_id<<" \n";
            char* buffer = new char[write_size];
            std::memset(buffer, 0, write_size);

            fileStream.seekp(0, std::ios::end);
            fileStream.write(buffer, write_size);
            fileStream.flush();
            
            num_pages = till_page_id+1;
        }
    }

};

using PageID = uint16_t;

class Policy {
public:
    virtual bool touch(PageID page_id) = 0;
    virtual PageID evict() = 0;
    virtual ~Policy() = default;
};

void printList(std::string list_name, const std::list<PageID>& myList) {
        std::cout << list_name << " :: ";
        for (const PageID& value : myList) {
            std::cout << value << ' ';
        }
        std::cout << '\n';
}

class LruPolicy : public Policy {
private:
    // List to keep track of the order of use
    std::list<PageID> lruList;

    // Map to find a page's iterator in the list efficiently
    std::unordered_map<PageID, std::list<PageID>::iterator> map;

    size_t cacheSize;

public:

    LruPolicy(size_t cacheSize) : cacheSize(cacheSize) {}

    bool touch(PageID page_id) override {
        //printList("LRU", lruList);

        bool found = false;
        // If page already in the list, remove it
        if (map.find(page_id) != map.end()) {
            found = true;
            lruList.erase(map[page_id]);
            map.erase(page_id);            
        }

        // If cache is full, evict
        if(lruList.size() == cacheSize){
            evict();
        }

        if(lruList.size() < cacheSize){
            // Add the page to the front of the list
            lruList.emplace_front(page_id);
            map[page_id] = lruList.begin();
        }

        return found;
    }

    PageID evict() override {
        // Evict the least recently used page
        PageID evictedPageId = INVALID_VALUE;
        if(lruList.size() != 0){
            evictedPageId = lruList.back();
            map.erase(evictedPageId);
            lruList.pop_back();
        }
        return evictedPageId;
    }

};

constexpr size_t MAX_PAGES_IN_MEMORY = 10;

class BufferManager {
private:
    using PageMap = std::unordered_map<PageID, SlottedPage>;

    StorageManager storage_manager;
    PageMap pageMap;
    std::unique_ptr<Policy> policy;

public:
    BufferManager(bool storage_manager_truncate_mode = true): 
        storage_manager(storage_manager_truncate_mode),
        policy(std::make_unique<LruPolicy>(MAX_PAGES_IN_MEMORY)) {
            storage_manager.extend(MAX_PAGES);
    }
    
    ~BufferManager() {
        for (auto& pair : pageMap) {
            flushPage(pair.first);
        }
    }

    SlottedPage& fix_page(int page_id) {
        auto it = pageMap.find(page_id);
        if (it != pageMap.end()) {
            policy->touch(page_id);
            return pageMap.find(page_id)->second;
        }

        if (pageMap.size() >= MAX_PAGES_IN_MEMORY) {
            auto evictedPageId = policy->evict();
            if(evictedPageId != INVALID_VALUE){
                // std::cout << "Evicting page " << evictedPageId << "\n";
                storage_manager.flush(evictedPageId, 
                                      pageMap[evictedPageId]);
            }
        }

        auto page = storage_manager.load(page_id);
        policy->touch(page_id);
        // std::cout << "Loading page: " << page_id << "\n";
        pageMap[page_id] = std::move(*page);
        return pageMap[page_id];
    }

    void flushPage(int page_id) {
        storage_manager.flush(page_id, pageMap[page_id]);
    }

    void extend(){
        storage_manager.extend();
    }
    
    size_t getNumPages(){
        return storage_manager.num_pages;
    }

};

template<typename KeyT, typename ValueT, typename ComparatorT, size_t PageSize>
class BTree {
    public:
        struct Node {
            /// The level in the tree.
            uint16_t level;

            /// The number of children.
            uint16_t count;

            /// TODO: Add additional members as needed
            uint64_t page_id;
            uint16_t splits;
            uint64_t parent;
            bool dirty;

            // Constructor
            Node(uint16_t level, uint16_t count)
                : level(level), count(count), page_id(0), splits(0),
                parent(0), dirty(false) {}

            /// Is the node a leaf node?
            bool is_leaf() const { return level == 0; }
            bool is_full(uint32_t capacity) const { return count >= capacity; }
            bool splits_needed(uint32_t capacity) const { return count > capacity; }
            void is_dirty() { dirty = true; }
        };

        struct InnerNode: public Node {
            /// The capacity of a node.
            /// TODO think about the capacity that the nodes have.
            static constexpr uint32_t kCapacity = 42;

            /// The keys.
            KeyT keys[kCapacity - 1];

            /// The children.
            uint64_t children[kCapacity];

            /// Constructor.
            InnerNode() : Node(0, 0) {}

            /// Get the index of the first key that is not less than than a provided key.
            /// @param[in] key          The key that should be searched.
            std::pair<uint32_t, bool> lower_bound(const KeyT &key) {
            // TODO: remove the below lines of code 
                // and add your implementation here
                // UNUSED(key);
                if (this->count == 0) {
                    return {0, false};
                }
                if (key < keys[0]) {
                    return {0, false};
                }
                for (uint32_t i = 0; i < static_cast<uint32_t>(this->count - 1); i++) {
                    if (key >= keys[i] && key < keys[i + 1]) {
                        return {i + 1, false};
                    }
                }
                return {this->count - 1, false};
            }

            /// Insert a key.
            /// @param[in] key          The separator that should be inserted.
            /// @param[in] split_page   The id of the split page that should be inserted.
            void insert(const KeyT &key, uint64_t split_page) {
            // TODO: remove the below lines of code 
                // and add your implementation here
            // UNUSED(key);
            // UNUSED(split_page);
            uint32_t position = lower_bound(key).first;
            for (uint32_t i = this -> count; i > position; i--) {
                keys[i] = keys[i-1];
                children[i + 1] = children[i];
            }
            keys[position] = key;
            children[position + 1] = split_page;
            this -> count++;
            this -> dirty = true;
            }

            /// Split the inner node.
            /// @param[in] inner_node       The inner node being split.
            /// @return                 The separator key.
            KeyT split(InnerNode* inner_node) {
                // TODO: remove the below lines of code 
                // and add your implementation here
                // UNUSED(inner_node);
                uint32_t mid = this->count / 2;
                KeyT separator = keys[mid];
                uint32_t j = 0;
                for (uint32_t i = mid + 1; i < this->count; i++) {
                    inner_node->keys[j] = keys[i];
                    inner_node->children[j] = children[i];
                    j++;
                }
                inner_node->children[j] = children[this->count];
                
                inner_node->count = this->count - mid - 1;
                inner_node->level = this->level;
                this->count = mid;

                this->dirty = true;
                inner_node->dirty = true;
                return separator;
            }

        };

        struct LeafNode: public Node {
            /// The capacity of a node.
            /// TODO think about the capacity that the nodes have.
            static constexpr uint32_t kCapacity = 42;

            /// The keys.
            KeyT keys[kCapacity];

            /// The values.
            ValueT values[kCapacity];

            /// Constructor.
            LeafNode() : Node(0, 0) {}

            uint32_t find_position(const KeyT &key) {
                uint32_t pos = 0;
                while (pos < this -> count && keys[pos] < key) {
                    pos++;
                }
                return pos;
            }

            /// Insert a key.
            /// @param[in] key          The key that should be inserted.
            /// @param[in] value        The value that should be inserted.
            void insert(const KeyT &key, const ValueT &value) {
                // HINT
                // UNUSED(key);
                // UNUSED(value);
                // uint32_t position = find_position(key);
                // if (position < this -> count && keys[position] == key) {
                //     values[position] = value;
                //     this -> dirty = true;
                //     return;
                // }

                // for (uint32_t i = this -> count; i > position; i--) {
                //     keys[i] = keys[i - 1];
                //     values[i] = values[i - 1];
                // }
                // keys[position] = key;
                // values[position] = value;
                // this -> count++;
                // this -> dirty = true;
                uint32_t position = find_position(key);
                if (position < this->count && keys[position] == key) {
                    values[position] = value;
                    this->dirty = true;
                    return;
                }
                for (uint32_t i = this->count; i > position; i--) {
                    keys[i] = keys[i - 1];
                    values[i] = values[i - 1];
                }
                keys[position] = key;
                values[position] = value;
                this->count++;
                this->dirty = true;
            }

            /// Erase a key.
            void erase(const KeyT &key) {
                // HINT
                // UNUSED(key);
                uint32_t position = find_position(key);
                if (position >= this -> count || keys[position] != key) {
                    return;
                }
                for (uint32_t i = position; i < static_cast<uint32_t>(this -> count - 1); i++) {
                    keys[i] = keys[i + 1];
                    values[i] = values[i + 1];
                }
                this -> count--;
                this -> dirty = true;
            }

            /// Split the leaf node.
            /// @param[in] leaf_node       The leaf node being split
            /// @return                 The separator key.
            KeyT split(LeafNode* leaf_node) {
                // HINT
                // UNUSED(leaf_node);
                // uint32_t mid = this -> count / 2;
                // uint32_t tmp = 0;
                // for (uint32_t i = mid; i < this -> count; i++) {
                //     leaf_node -> keys[tmp] = keys[i];
                //     leaf_node -> values[tmp] = values[i];
                //     tmp++;
                // }
                // leaf_node -> count = this -> count - mid;
                // leaf_node -> level = 0;
                // this -> count = mid;
                // this -> dirty = true;
                // leaf_node -> dirty = true;
                // return leaf_node -> keys[0];
                uint32_t mid = this->count / 2;
                uint32_t j = 0;
                for (uint32_t i = mid; i < this->count; i++) {
                    leaf_node->keys[j] = keys[i];
                    leaf_node->values[j] = values[i];
                    j++;
                }
                leaf_node->count = j;
                this->count = mid;
                return leaf_node->keys[0];
            }

        };

        /// The root.
        std::optional<uint64_t> root;

        /// The buffer manager
        BufferManager& buffer_manager;

        /// Next page id.
        /// You don't need to worry about about the page allocation.
        /// (Neither fragmentation, nor persisting free-space bitmaps)
        /// Just increment the next_page_id whenever you need a new page.
        uint64_t next_page_id;

        /// Constructor.
        BTree(BufferManager &buffer_manager): buffer_manager(buffer_manager) {
            // TODO
            next_page_id = 1;
            root = std::nullopt;

            while (buffer_manager.getNumPages() < MAX_PAGES) {
                buffer_manager.extend();
            }
        }

        /// Lookup an entry in the tree.
        /// @param[in] key      The key that should be searched.
        std::optional<ValueT> lookup(const KeyT &key) {
            // TODO
            // UNUSED(key);
            // return std::optional<ValueT>();
            if (!root.has_value()) {
                return std::nullopt;
            }
            uint64_t curr = *root;
            while (1) {
                SlottedPage& page = buffer_manager.fix_page(curr);
                Node* node = reinterpret_cast<Node*>(page.page_data.get());
                
                if (node->is_leaf()) {
                    LeafNode* leaf = reinterpret_cast<LeafNode*>(node);
                    uint32_t position = leaf->find_position(key);
                    if (position < leaf->count && leaf->keys[position] == key) {
                        return leaf->values[position];
                    }
                    return std::nullopt;
                } else {
                    InnerNode* inner = reinterpret_cast<InnerNode*>(node);
                    uint32_t position = inner->lower_bound(key).first;
                    curr = inner->children[position];
                }
            }
        }

        /// Erase an entry in the tree.
        /// @param[in] key      The key that should be searched.
        void erase(const KeyT &key) {
            // TODO
            // UNUSED(key);
            if (!root.has_value()) {
                return;
            }
            uint64_t curr = *root;
            while (1) {
                SlottedPage& page = buffer_manager.fix_page(curr);
                Node* node = reinterpret_cast<Node*>(page.page_data.get());

                if (node -> is_leaf()) {
                    LeafNode* leaf = reinterpret_cast<LeafNode*>(node);
                    leaf -> erase(key);
                    return;
                } else {
                    InnerNode* inner = reinterpret_cast<InnerNode*>(node);
                    uint32_t position = inner -> lower_bound(key).first;
                    curr = inner -> children[position];
                }
            }
        }

        /// Inserts a new entry into the tree.
        /// @param[in] key      The key that should be inserted.
        /// @param[in] value    The value that should be inserted.
        void insert(const KeyT &key, const ValueT &value) {
            // TODO
            // UNUSED(key);
            // UNUSED(value);
            if (!root.has_value()) {
                uint64_t page_id = next_page_id++;
                auto& page = buffer_manager.fix_page(page_id);
                auto leaf = reinterpret_cast<LeafNode*>(page.page_data.get());
                *leaf = LeafNode();
                leaf->insert(key, value);
                root = page_id;
                return;
            }

            uint64_t curr = *root;
            std::vector<uint64_t> path;

            while (1) {
                path.push_back(curr);
                SlottedPage& page = buffer_manager.fix_page(curr);
                Node* node = reinterpret_cast<Node*>(page.page_data.get());

                if (node->is_leaf()) {
                    LeafNode* leaf = reinterpret_cast<LeafNode*>(node);
                    if (!leaf->is_full(LeafNode::kCapacity)) {
                        leaf->insert(key, value);
                        return;
                    }

                    uint64_t new_page_id = next_page_id++;
                    SlottedPage& new_page = buffer_manager.fix_page(new_page_id);
                    auto new_leaf = reinterpret_cast<LeafNode*>(new_page.page_data.get());
                    *new_leaf = LeafNode();

                    KeyT separator = leaf->split(new_leaf);
                    if (key >= separator) {
                        new_leaf->insert(key, value);
                    } else {
                        leaf->insert(key, value);
                    }
                    if (path.size() == 1) {
                        uint64_t new_root_id = next_page_id++;
                        auto& new_root_page = buffer_manager.fix_page(new_root_id);
                        auto new_root = reinterpret_cast<InnerNode*>(new_root_page.page_data.get());
                        *new_root = InnerNode();
                        new_root->level = 1;
                        new_root->children[0] = curr;
                        new_root->keys[0] = separator;
                        new_root->children[1] = new_page_id;
                        new_root->count = 2;
                        root = new_root_id;
                    } else {
                        path.pop_back();
                        uint64_t parent_id = path.back();
                        auto& parent_page = buffer_manager.fix_page(parent_id);
                        auto parent = reinterpret_cast<InnerNode*>(parent_page.page_data.get());
                        
                        uint32_t child_idx = 0;
                        while (child_idx < parent->count && parent->children[child_idx] != curr) {
                            child_idx++;
                        }
                        
                        for (uint32_t i = parent->count; i > child_idx + 1; i--) {
                            parent->children[i] = parent->children[i-1];
                            parent->keys[i-1] = parent->keys[i-2];
                        }
                        parent->keys[child_idx] = separator;
                        parent->children[child_idx + 1] = new_page_id;
                        parent->count++;
                    }
                    return;
                } else {
                    InnerNode* inner = reinterpret_cast<InnerNode*>(node);
                    uint32_t position = inner->lower_bound(key).first;
                    curr = inner->children[position];
                }
            }

        }

        void insertIntoParent(std::vector<uint64_t>& path, KeyT separator, uint64_t new_page_id) {
            if (path.size() == 1) {
                uint64_t new_root_id = next_page_id++;
                auto& new_root_page = buffer_manager.fix_page(new_root_id);
                auto new_root = reinterpret_cast<InnerNode*>(new_root_page.page_data.get());
                *new_root = InnerNode();
                new_root->level = 1;
                new_root->children[0] = *root;
                new_root->keys[0] = separator;
                new_root->children[1] = new_page_id;
                new_root->count = 1;
                root = new_root_id;
            } else {
                path.pop_back();
                uint64_t parent_id = path.back();
                auto& parent_page = buffer_manager.fix_page(parent_id);
                auto parent = reinterpret_cast<InnerNode*>(parent_page.page_data.get());
                uint32_t pos = parent->lower_bound(separator).first;
                
                for (uint32_t i = parent->count; i > pos; i--) {
                    parent->keys[i] = parent->keys[i - 1];
                    parent->children[i + 1] = parent->children[i];
                }
                parent->keys[pos] = separator;
                parent->children[pos + 1] = new_page_id;
                parent->count++;
                
                if (parent->is_full(InnerNode::kCapacity)) {
                    uint64_t new_inner_id = next_page_id++;
                    auto& new_inner_page = buffer_manager.fix_page(new_inner_id);
                    auto new_inner = reinterpret_cast<InnerNode*>(new_inner_page.page_data.get());
                    *new_inner = InnerNode();
                    
                    KeyT new_separator = parent->split(new_inner);
                    insertIntoParent(path, new_separator, new_inner_id);
                }
            }
        }
};

int main(int argc, char* argv[]) {
    bool execute_all = false;
    std::string selected_test = "-1";

    if(argc < 2) {
        execute_all = true;
    } else {
        selected_test = argv[1];
    }

    using BTree = BTree<uint64_t, uint64_t, std::less<uint64_t>, 1024>;

    // Test 1: InsertEmptyTree
    if(execute_all || selected_test == "1") {
        std::cout<<"...Starting Test 1"<<std::endl;
        BufferManager buffer_manager;
        BTree tree(buffer_manager);

        ASSERT_WITH_MESSAGE(tree.root.has_value() == false, 
            "tree.root is not nullptr");

        tree.insert(42, 21);

        ASSERT_WITH_MESSAGE(tree.root.has_value(), 
            "tree.root is still nullptr after insertion");
        
        std::string test = "inserting an element into an empty B-Tree";

        // Fix root page and obtain root node pointer
        SlottedPage* root_page = &buffer_manager.fix_page(*tree.root);
        auto root_node = reinterpret_cast<BTree::Node*>(root_page->page_data.get());

        ASSERT_WITH_MESSAGE(root_node->is_leaf() == true, 
            test + " does not create a leaf node.");
        ASSERT_WITH_MESSAGE(root_node->count == 1,
            test + " does not create a leaf node with count = 1.");

        std::cout << "\033[1m\033[32mPassed: Test 1\033[0m" << std::endl;
    }

    // Test 2: InsertLeafNode
    if(execute_all || selected_test == "2") {
        std::cout<<"...Starting Test 2"<<std::endl;
        BufferManager buffer_manager;
        BTree tree( buffer_manager);
        
        ASSERT_WITH_MESSAGE(tree.root.has_value() == false, 
            "tree.root is not nullptr");

        for (auto i = 0ul; i < BTree::LeafNode::kCapacity; ++i) {
            tree.insert(i, 2 * i);
        }
        ASSERT_WITH_MESSAGE(tree.root.has_value(), 
            "tree.root is still nullptr after insertion");

        std::string test = "inserting BTree::LeafNode::kCapacity elements into an empty B-Tree";

        SlottedPage* root_page = &buffer_manager.fix_page(*tree.root);
        auto root_node = reinterpret_cast<BTree::Node*>(root_page->page_data.get());
        auto root_inner_node = static_cast<BTree::InnerNode*>(root_node);

        ASSERT_WITH_MESSAGE(root_node->is_leaf() == true, 
            test + " creates an inner node as root.");
        ASSERT_WITH_MESSAGE(root_inner_node->count == BTree::LeafNode::kCapacity,
            test + " does not store all elements.");

        std::cout << "\033[1m\033[32mPassed: Test 2\033[0m" << std::endl;
    }

    // Test 3: InsertLeafNodeSplit
    if (execute_all || selected_test == "3") {
        std::cout << "...Starting Test 3" << std::endl;
        BufferManager buffer_manager;
        BTree tree(buffer_manager);

        ASSERT_WITH_MESSAGE(tree.root.has_value() == false, 
            "tree.root is not nullptr");

        for (auto i = 0ul; i < BTree::LeafNode::kCapacity; ++i) {
            tree.insert(i, 2 * i);
        }
        ASSERT_WITH_MESSAGE(tree.root.has_value(), 
            "tree.root is still nullptr after insertion");
        
        SlottedPage* root_page = &buffer_manager.fix_page(*tree.root);
        auto root_node = reinterpret_cast<BTree::Node*>(root_page->page_data.get());
        auto root_inner_node = static_cast<BTree::InnerNode*>(root_node);
        
        assert(root_inner_node->is_leaf());
        assert(root_inner_node->count == BTree::LeafNode::kCapacity);

        // Let there be a split...
        tree.insert(424242, 42);

        std::string test =
            "inserting BTree::LeafNode::kCapacity + 1 elements into an empty B-Tree";
        
        ASSERT_WITH_MESSAGE(tree.root.has_value() != false, test + " removes the root :-O");

        SlottedPage* root_page1 = &buffer_manager.fix_page(*tree.root);
        root_node = reinterpret_cast<BTree::Node*>(root_page1->page_data.get());
        root_inner_node = static_cast<BTree::InnerNode*>(root_node);

        ASSERT_WITH_MESSAGE(root_inner_node->is_leaf() == false, 
            test + " does not create a root inner node");
        ASSERT_WITH_MESSAGE(root_inner_node->count == 2, 
            test + " creates a new root with count != 2");

        std::cout << "\033[1m\033[32mPassed: Test 3\033[0m" << std::endl;
    }

    // Test 4: LookupEmptyTree
    if (execute_all || selected_test == "4") {
        std::cout << "...Starting Test 4" << std::endl;
        BufferManager buffer_manager;
        BTree tree(buffer_manager);

        std::string test = "searching for a non-existing element in an empty B-Tree";

        ASSERT_WITH_MESSAGE(tree.lookup(42).has_value() == false, 
            test + " seems to return something :-O");

        std::cout << "\033[1m\033[32mPassed: Test 4\033[0m" << std::endl;
    }

    // Test 5: LookupSingleLeaf
    if (execute_all || selected_test == "5") {
        std::cout << "...Starting Test 5" << std::endl;
        BufferManager buffer_manager;
        BTree tree(buffer_manager);

        // Fill one page
        for (auto i = 0ul; i < BTree::LeafNode::kCapacity; ++i) {
            tree.insert(i, 2 * i);
            ASSERT_WITH_MESSAGE(tree.lookup(i).has_value(), 
                "searching for the just inserted key k=" + std::to_string(i) + " yields nothing");
        }

        // Lookup all values
        for (auto i = 0ul; i < BTree::LeafNode::kCapacity; ++i) {
            auto v = tree.lookup(i);
            ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(i) + " is missing");
            ASSERT_WITH_MESSAGE(*v == 2 * i, "key=" + std::to_string(i) + " should have the value v=" + std::to_string(2 * i));
        }

        std::cout << "\033[1m\033[32mPassed: Test 5\033[0m" << std::endl;
    }

    // Test 6: LookupSingleSplit
    if (execute_all || selected_test == "6") {
        std::cout << "...Starting Test 6" << std::endl;
        BufferManager buffer_manager;
        BTree tree(buffer_manager);

        // Insert values
        for (auto i = 0ul; i < BTree::LeafNode::kCapacity; ++i) {
            tree.insert(i, 2 * i);
        }

        tree.insert(BTree::LeafNode::kCapacity, 2 * BTree::LeafNode::kCapacity);
        ASSERT_WITH_MESSAGE(tree.lookup(BTree::LeafNode::kCapacity).has_value(),
            "searching for the just inserted key k=" + std::to_string(BTree::LeafNode::kCapacity + 1) + " yields nothing");

        // Lookup all values
        for (auto i = 0ul; i < BTree::LeafNode::kCapacity + 1; ++i) {
            auto v = tree.lookup(i);
            ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(i) + " is missing");
            ASSERT_WITH_MESSAGE(*v == 2 * i, 
                "key=" + std::to_string(i) + " should have the value v=" + std::to_string(2 * i));
        }

        std::cout << "\033[1m\033[32mPassed: Test 6\033[0m" << std::endl;
    }

    // Test 7: LookupMultipleSplitsIncreasing
    if (execute_all || selected_test == "7") {
        std::cout << "...Starting Test 7" << std::endl;
        BufferManager buffer_manager;
        BTree tree(buffer_manager);
        auto n = 40 * BTree::LeafNode::kCapacity;

        // Insert values
        for (auto i = 0ul; i < n; ++i) {
            tree.insert(i, 2 * i);
            ASSERT_WITH_MESSAGE(tree.lookup(i).has_value(),
                "searching for the just inserted key k=" + std::to_string(i) + " yields nothing");
        }

        // Lookup all values
        for (auto i = 0ul; i < n; ++i) {
            auto v = tree.lookup(i);
            ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(i) + " is missing");
            ASSERT_WITH_MESSAGE(*v == 2 * i,
                "key=" + std::to_string(i) + " should have the value v=" + std::to_string(2 * i));
        }
        std::cout << "\033[1m\033[32mPassed: Test 7\033[0m" << std::endl;
    }

    // Test 8: LookupMultipleSplitsDecreasing
    if (execute_all || selected_test == "8") {
        std::cout << "...Starting Test 8" << std::endl;
        BufferManager buffer_manager;
        BTree tree(buffer_manager);
        auto n = 10 * BTree::LeafNode::kCapacity;

        // Insert values
        for (auto i = n; i > 0; --i) {
            tree.insert(i, 2 * i);
            ASSERT_WITH_MESSAGE(tree.lookup(i).has_value(),
                "searching for the just inserted key k=" + std::to_string(i) + " yields nothing");
        }

        // Lookup all values
        for (auto i = n; i > 0; --i) {
            auto v = tree.lookup(i);
            ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(i) + " is missing");
            ASSERT_WITH_MESSAGE(*v == 2 * i,
                "key=" + std::to_string(i) + " should have the value v=" + std::to_string(2 * i));
        }

        std::cout << "\033[1m\033[32mPassed: Test 8\033[0m" << std::endl;
    }

    // Test 9: LookupRandomNonRepeating
    if (execute_all || selected_test == "9") {
        std::cout << "...Starting Test 9" << std::endl;
        BufferManager buffer_manager;
        BTree tree(buffer_manager);
        auto n = 10 * BTree::LeafNode::kCapacity;

        // Generate random non-repeating key sequence
        std::vector<uint64_t> keys(n);
        std::iota(keys.begin(), keys.end(), n);
        std::mt19937_64 engine(0);
        std::shuffle(keys.begin(), keys.end(), engine);

        // Insert values
        for (auto i = 0ul; i < n; ++i) {
            tree.insert(keys[i], 2 * keys[i]);
            ASSERT_WITH_MESSAGE(tree.lookup(keys[i]).has_value(),
                "searching for the just inserted key k=" + std::to_string(keys[i]) +
                " after i=" + std::to_string(i) + " inserts yields nothing");
        }

        // Lookup all values
        for (auto i = 0ul; i < n; ++i) {
            auto v = tree.lookup(keys[i]);
            ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(keys[i]) + " is missing");
            ASSERT_WITH_MESSAGE(*v == 2 * keys[i],
                "key=" + std::to_string(keys[i]) + " should have the value v=" + std::to_string(2 * keys[i]));
        }

        std::cout << "\033[1m\033[32mPassed: Test 9\033[0m" << std::endl;
    }

    // Test 10: LookupRandomRepeating
    if (execute_all || selected_test == "10") {
        std::cout << "...Starting Test 10" << std::endl;
        BufferManager buffer_manager;
        BTree tree(buffer_manager);
        auto n = 10 * BTree::LeafNode::kCapacity;

        // Insert & updated 100 keys at random
        std::mt19937_64 engine{0};
        std::uniform_int_distribution<uint64_t> key_distr(0, 99);
        std::vector<uint64_t> values(100);

        for (auto i = 1ul; i < n; ++i) {
            uint64_t rand_key = key_distr(engine);
            values[rand_key] = i;
            tree.insert(rand_key, i);

            auto v = tree.lookup(rand_key);
            ASSERT_WITH_MESSAGE(v.has_value(),
                "searching for the just inserted key k=" + std::to_string(rand_key) +
                " after i=" + std::to_string(i - 1) + " inserts yields nothing");
            ASSERT_WITH_MESSAGE(*v == i,
                "overwriting k=" + std::to_string(rand_key) + " with value v=" + std::to_string(i) +
                " failed");
        }

        // Lookup all values
        for (auto i = 0ul; i < 100; ++i) {
            if (values[i] == 0) {
                continue;
            }
            auto v = tree.lookup(i);
            ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(i) + " is missing");
            ASSERT_WITH_MESSAGE(*v == values[i],
                "key=" + std::to_string(i) + " should have the value v=" + std::to_string(values[i]));
        }

        std::cout << "\033[1m\033[32mPassed: Test 10\033[0m" << std::endl;
    }

    // Test 11: Erase
    if (execute_all || selected_test == "11") {
        std::cout << "...Starting Test 11" << std::endl;
        BufferManager buffer_manager;
        BTree tree(buffer_manager);

        // Insert values
        for (auto i = 0ul; i < 2 * BTree::LeafNode::kCapacity; ++i) {
            tree.insert(i, 2 * i);
        }

        // Iteratively erase all values
        for (auto i = 0ul; i < 2 * BTree::LeafNode::kCapacity; ++i) {
            ASSERT_WITH_MESSAGE(tree.lookup(i).has_value(), "k=" + std::to_string(i) + " was not in the tree");
            tree.erase(i);
            ASSERT_WITH_MESSAGE(!tree.lookup(i), "k=" + std::to_string(i) + " was not removed from the tree");
        }
        std::cout << "\033[1m\033[32mPassed: Test 11\033[0m" << std::endl;
    }

    // Test 12: Persistant Btree
    if (execute_all || selected_test == "12") {
        std::cout << "...Starting Test 12" << std::endl;
        unsigned long n =  10 * BTree::LeafNode::kCapacity;

        // Build a tree
        {
            BufferManager buffer_manager;
            BTree tree(buffer_manager);
            
            // Insert values
            for (auto i = 0ul; i < n; ++i) {
                tree.insert(i, 2 * i);
                ASSERT_WITH_MESSAGE(tree.lookup(i).has_value(),
                    "searching for the just inserted key k=" + std::to_string(i) + " yields nothing");
            }

            // Lookup all values
            for (auto i = 0ul; i < n; ++i) {
                auto v = tree.lookup(i);
                ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(i) + " is missing");
                ASSERT_WITH_MESSAGE(*v == 2 * i,
                    "key=" + std::to_string(i) + " should have the value v=" + std::to_string(2 * i));
            }
        }

        // recreate the buffer manager and check for existence of the tree
        {
            BufferManager buffer_manager(false);
            BTree tree(buffer_manager);

            // Lookup all values
            for (auto i = 0ul; i < n; ++i) {
                auto v = tree.lookup(i);
                ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(i) + " is missing");
                ASSERT_WITH_MESSAGE(*v == 2 * i,
                    "key=" + std::to_string(i) + " should have the value v=" + std::to_string(2 * i));
            }
        }

        std::cout << "\033[1m\033[32mPassed: Test 12\033[0m" << std::endl;
    }

    return 0;
}