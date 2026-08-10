require "./spec_helper"
require "../src/mqtt/v5/property"

module MQTT::V5
  describe Properties do
    describe "round trips" do
      it "encodes every data type the way the specification says" do
        props = Properties.new
        props.set_byte(PropertyId::PayloadFormatIndicator, 1_u8)
        props.set_four_byte(PropertyId::MessageExpiryInterval, 3600_u32)
        props.set_string(PropertyId::ContentType, "application/json")
        props.set_two_byte(PropertyId::TopicAlias, 7_u16)
        props.set_binary(PropertyId::CorrelationData, Bytes[1, 2, 3, 4])
        props.add_pair(PropertyId::UserProperty, "tenant", "acme")
        props.add_variable_byte(PropertyId::SubscriptionIdentifier, 321_u32)

        wire = props.to_slice
        # 01 01                      payload format indicator
        # 02 00000e10                message expiry, 3600
        # 03 0010 "application/json" content type
        # 23 0007                    topic alias
        # 09 0004 01020304           correlation data
        # 26 0006 "tenant" 0004 "acme"
        # 0b c102                    subscription identifier, 321
        # 2 + 5 + 19 + 3 + 7 + 15 + 3 == 54 content bytes, in a one byte prefix
        props.content_length.should eq 54
        wire.hexstring.should start_with "36"
        wire.hexstring.should contain "0300106170706c69636174696f6e2f6a736f6e"
        wire.hexstring.should contain "26000674656e616e74000461636d65"
        wire.hexstring.should contain "0bc102"

        decoded = IO::Memory.new(wire).read_bytes(Properties)
        decoded.declared_length.should eq props.content_length
        decoded.byte(PropertyId::PayloadFormatIndicator).should eq 1_u8
        decoded.four_byte(PropertyId::MessageExpiryInterval).should eq 3600_u32
        decoded.string(PropertyId::ContentType).should eq "application/json"
        decoded.two_byte(PropertyId::TopicAlias).should eq 7_u16
        decoded.binary(PropertyId::CorrelationData).should eq Bytes[1, 2, 3, 4]
        decoded.pairs(PropertyId::UserProperty).should eq [{"tenant", "acme"}]
        decoded.variable_bytes(PropertyId::SubscriptionIdentifier).should eq [321_u32]
        decoded.to_slice.should eq wire
      end

      it "encodes an empty section as a single zero byte" do
        props = Properties.new
        props.to_slice.should eq Bytes[0]
        props.total_size.should eq 1
        props.empty?.should be_true

        IO::Memory.new(props.to_slice).read_bytes(Properties).properties.should be_empty
      end

      # the length prefix is itself a variable byte integer, so a section over
      # 127 bytes exercises a code path a small one never reaches
      it "encodes a section needing a multi byte length prefix" do
        props = Properties.new
        12.times { |i| props.add_pair(PropertyId::UserProperty, "key#{i}".ljust(10, 'x'), "value#{i}".ljust(10, 'y')) }

        props.content_length.should eq 300
        wire = props.to_slice
        wire[0, 2].should eq Bytes[0xAC, 0x02]
        wire.size.should eq 302

        decoded = IO::Memory.new(wire).read_bytes(Properties)
        decoded.declared_length.should eq 300
        decoded.properties.size.should eq 12
        decoded.to_slice.should eq wire
      end

      it "stops at the declared length when embedded in a larger packet" do
        props = Properties.new
        props.set_two_byte(PropertyId::TopicAlias, 9_u16)

        io = IO::Memory.new
        io.write_bytes(props)
        io.write_bytes(0xBEEF_u16, IO::ByteFormat::BigEndian)
        io.rewind

        decoded = io.read_bytes(Properties)
        decoded.two_byte(PropertyId::TopicAlias).should eq 9_u16
        # the following field must still be readable
        io.read_bytes(UInt16, IO::ByteFormat::BigEndian).should eq 0xBEEF_u16
      end

      it "round trips the full variable byte integer range" do
        {0_u32, 1_u32, 127_u32, 128_u32, 16_383_u32, 16_384_u32, 2_097_151_u32,
         2_097_152_u32, 268_435_455_u32}.each do |value|
          props = Properties.new
          props.add_variable_byte(PropertyId::SubscriptionIdentifier, value)
          decoded = IO::Memory.new(props.to_slice).read_bytes(Properties)
          decoded.variable_bytes(PropertyId::SubscriptionIdentifier).should eq [value]
        end
      end
    end

    describe "typed access" do
      it "replaces rather than duplicating a single use property" do
        props = Properties.new
        props.set_two_byte(PropertyId::TopicAlias, 7_u16)
        props.set_two_byte(PropertyId::TopicAlias, 9_u16)

        props.properties.size.should eq 1
        props.two_byte(PropertyId::TopicAlias).should eq 9_u16
      end

      it "removes a property when set to nil" do
        props = Properties.new
        props.set_string(PropertyId::ContentType, "text/plain")
        props.set_string(PropertyId::ContentType, nil)

        props.string(PropertyId::ContentType).should be_nil
        props.properties.should be_empty
      end

      it "appends repeatable properties" do
        props = Properties.new
        props.add_pair(PropertyId::UserProperty, "a", "1")
        props.add_pair(PropertyId::UserProperty, "b", "2")

        props.pairs(PropertyId::UserProperty).should eq [{"a", "1"}, {"b", "2"}]
      end

      it "returns nil for a property that isn't present" do
        Properties.new.string(PropertyId::ContentType).should be_nil
        Properties.new.pairs(PropertyId::UserProperty).should be_empty
      end
    end

    describe "validation" do
      it "rejects a property that is illegal for the packet" do
        props = Properties.new
        props.set_four_byte(PropertyId::MessageExpiryInterval, 1_u32)

        expect_raises(MQTT::ProtocolError, /not a valid property for PUBLISH/) do
          props.validate!([PropertyId::PayloadFormatIndicator], "PUBLISH")
        end
      end

      it "rejects a repeated single use property" do
        props = Properties.new
        props.properties = [1_u16, 2_u16].map do |alias_id|
          property = Property.new
          property.identifier = PropertyId::TopicAlias
          property.two_byte_value = alias_id
          property
        end

        expect_raises(MQTT::ProtocolError, /may only appear once/) do
          props.validate!([PropertyId::TopicAlias], "PUBLISH")
        end
      end

      it "allows a repeated repeatable property" do
        props = Properties.new
        props.add_pair(PropertyId::UserProperty, "a", "1")
        props.add_pair(PropertyId::UserProperty, "b", "2")
        props.add_variable_byte(PropertyId::SubscriptionIdentifier, 1_u32)
        props.add_variable_byte(PropertyId::SubscriptionIdentifier, 2_u32)

        props.validate!([PropertyId::UserProperty, PropertyId::SubscriptionIdentifier], "PUBLISH")
          .should be props
      end
    end

    describe "malformed input" do
      it "fails when the declared length overruns the data" do
        # says 20 bytes of properties follow, supplies 2
        expect_raises(Exception) do
          IO::Memory.new(Bytes[20, 0x23, 0x00]).read_bytes(Properties)
        end
      end

      it "fails on an identifier the specification does not define" do
        expect_raises(Exception) do
          IO::Memory.new(Bytes[2, 0x7F, 0x00]).read_bytes(Properties)
        end
      end
    end

    describe PropertyId do
      it "maps every identifier to a data type" do
        PropertyId.each do |id|
          id.data_type.should be_a PropertyType
        end
      end

      it "knows which properties may repeat" do
        PropertyId::UserProperty.repeatable?.should be_true
        PropertyId::SubscriptionIdentifier.repeatable?.should be_true
        PropertyId::TopicAlias.repeatable?.should be_false
      end
    end
  end
end
