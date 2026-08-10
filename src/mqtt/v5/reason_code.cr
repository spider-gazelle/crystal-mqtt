require "../base"

module MQTT
  module V5
    # 5.0 replaces 3.1.1's handful of return codes with a single reason code
    # space shared across CONNACK, the PUBLISH acknowledgements, SUBACK,
    # UNSUBACK, DISCONNECT and AUTH.
    #
    # NOTE:: anything below 0x80 is a success. `Success`, `GrantedQoS0` and
    # `NormalDisconnection` are all zero, they simply read differently
    # depending on which packet carried them
    enum ReasonCode : UInt8
      Success                             = 0x00
      GrantedQoS1                         = 0x01
      GrantedQoS2                         = 0x02
      DisconnectWithWillMessage           = 0x04
      NoMatchingSubscribers               = 0x10
      NoSubscriptionExisted               = 0x11
      ContinueAuthentication              = 0x18
      ReAuthenticate                      = 0x19
      UnspecifiedError                    = 0x80
      MalformedPacket                     = 0x81
      ProtocolError                       = 0x82
      ImplementationSpecificError         = 0x83
      UnsupportedProtocolVersion          = 0x84
      ClientIdentifierNotValid            = 0x85
      BadUserNameOrPassword               = 0x86
      NotAuthorized                       = 0x87
      ServerUnavailable                   = 0x88
      ServerBusy                          = 0x89
      Banned                              = 0x8A
      ServerShuttingDown                  = 0x8B
      BadAuthenticationMethod             = 0x8C
      KeepAliveTimeout                    = 0x8D
      SessionTakenOver                    = 0x8E
      TopicFilterInvalid                  = 0x8F
      TopicNameInvalid                    = 0x90
      PacketIdentifierInUse               = 0x91
      PacketIdentifierNotFound            = 0x92
      ReceiveMaximumExceeded              = 0x93
      TopicAliasInvalid                   = 0x94
      PacketTooLarge                      = 0x95
      MessageRateTooHigh                  = 0x96
      QuotaExceeded                       = 0x97
      AdministrativeAction                = 0x98
      PayloadFormatInvalid                = 0x99
      RetainNotSupported                  = 0x9A
      QoSNotSupported                     = 0x9B
      UseAnotherServer                    = 0x9C
      ServerMoved                         = 0x9D
      SharedSubscriptionsNotSupported     = 0x9E
      ConnectionRateExceeded              = 0x9F
      MaximumConnectTime                  = 0xA0
      SubscriptionIdentifiersNotSupported = 0xA1
      WildcardSubscriptionsNotSupported   = 0xA2

      # MQTT-2.4, everything below 0x80 is a success
      def success? : Bool
        value < 0x80_u8
      end

      def error? : Bool
        !success?
      end

      # The QoS a SUBACK granted, or nil if the filter was rejected
      def granted_qos : QoS?
        return unless success?
        QoS.from_value?(value)
      end

      # Whether reconnecting with the same settings could plausibly work.
      # Authentication, identity and protocol failures will not, and a redirect
      # wants us somewhere else entirely
      def fatal? : Bool
        case self
        when .malformed_packet?, .protocol_error?, .unsupported_protocol_version?,
             .client_identifier_not_valid?, .bad_user_name_or_password?, .not_authorized?,
             .banned?, .bad_authentication_method?, .server_moved?, .use_another_server?,
             .retain_not_supported?, .qo_s_not_supported?,
        # someone else connected with our client id. Reconnecting kicks
        # them straight back off, and they reconnect and kick us — two
        # clients sharing an id fight forever. Stay down instead
             .session_taken_over?
          true
        else
          false
        end
      end

      def description : String
        to_s.underscore.tr("_", " ")
      end
    end

    # Reason codes arrive from a remote host, so an unrecognised value must not
    # be an exception. The raw byte is always kept
    module ReasonCoded
      def reason_code : ReasonCode?
        ReasonCode.from_value?(raw_reason_code)
      end

      def reason_code=(code : ReasonCode) : ReasonCode
        self.raw_reason_code = code.value
        code
      end

      # Unknown codes are treated by their range, as the specification requires
      def success? : Bool
        raw_reason_code < 0x80_u8
      end

      def error? : Bool
        !success?
      end

      def reason_description : String
        reason_code.try(&.description) || "unknown reason code 0x#{raw_reason_code.to_s(16)}"
      end
    end
  end # V5
end   # MQTT
