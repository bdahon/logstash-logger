module LogStashLogger
  module Device
    class GooglePubsub < Connectable
      def initialize(opts)
        super

        @key_file = opts[:key_file]
        if @key_file
          credentials = MultiJson.load(key_file)
          gcloud = Google::Cloud.new(credentials["project_id"], credentials)
        else
          gcloud = Google::Cloud.new
        end

        @topic = opts[:topic]
      end

      def connect
        @io = pubsub.topic @topic
      end

      def with_connection
        connect unless connected?
        yield
      end

      def write_batch(messages)
        topic ||= @topic
        with_connection do
          messages.each { |message| @io.publish message }
        end
      end

      def write_one(message)
        write_batch([message], topic)
      end
    end
  end
end
