#include <iostream>
#include <fstream>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>
#include <folly/FBString.h>
#include <librdkafka/rdkafkacpp.h>

static uint64_t count = 0;

void process_chunk(folly::fbstring &chunk,
                   RdKafka::Producer *producer,
                   RdKafka::Topic *topic) {
  size_t start = 0;
  size_t match = 0;
  size_t size = chunk.size();

  while ((match = chunk.find('\n', start)) != std::string::npos) {
    void *start_ptr = (void*) &(chunk[start]);
    size_t length = match - start;
    std::string key((char*) start_ptr, length);
    producer->produce(topic, 0, RdKafka::Producer::RK_MSG_COPY, start_ptr,
                      length, &key, NULL);
    start = match + 1;

    if (++count % 1000000 == 0) {
      std::cout << (count / 1000000) << std::endl;
    }
  }

  if (start < size) {
    chunk = chunk.substr(start);
  } else {
    chunk = "";
  }
}

int main(int argc, char **argv) {
  if (argc != 4) {
    std::cerr << "Usage: ./kafka_fill [file_name] [topic_name] [broker_addr]\n";
    return 1;
  }
  const std::string file_name(argv[1]);
  const std::string topic_name(argv[2]);
  const std::string broker_addr(argv[3]);

  int page_size = getpagesize();
  char *buf = new char[page_size];
  folly::fbstring buffer(page_size * 2, 0);
  std::string err;

  std::fstream data_file(file_name, std::fstream::in);

  auto *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  auto *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

  auto status = conf->set("metadata.broker.list", broker_addr, err);
  if (status != RdKafka::Conf::CONF_OK) {
    throw std::runtime_error(err);
  }

  auto *producer = RdKafka::Producer::create(conf, err);

  if (!producer) {
    throw std::runtime_error(err);
  }

  auto *topic = RdKafka::Topic::create(producer, topic_name, tconf, err);

  if (!topic) {
    throw std::runtime_error(err);
  }

  while (!data_file.eof()) {
    data_file.read(buf, page_size);
    buffer.append(buf, data_file.gcount());
    process_chunk(buffer, producer, topic);
  }

  int outq;
  while ((outq = producer->outq_len()) > 0) {
    std::cout << "Waiting to drain queue: " << outq << std::endl;
    producer->poll(5000);
  }

  delete topic;
  delete producer;
  delete buf;

  return 0;
}
