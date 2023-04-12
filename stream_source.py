'''
{"type":"ad", "content":{"ts": 1581098400, "adId": "ad-123"}}
{"type":"ad", "content":{"ts": 1581098403, "adId": "ad-124"}}
{"type":"ad", "content":{"ts": 1581098406, "adId": "ad-125"}}
{"type":"ad", "content":{"ts": 1581098409, "adId": "ad-126"}}
{"type":"ad", "content":{"ts": 1581098412, "adId": "ad-127"}}
{"type":"ad", "content":{"ts": 1581098416, "adId": "ad-128"}}


PATH=$PATH:/home/ec2-user/confluent-5.5.0/bin
kafka-console-producer \
--broker-list ec2-52-66-45-236.ap-south-1.compute.amazonaws.com:9092 \
--topic clicks

{"type":"click", "content":{"ts": 1581098400, "adId": "ad-123", "userId": "user-001"}}
{"type":"click", "content":{"ts": 1581098404, "adId": "ad-123", "userId": "user-001"}}
{"type":"click", "content":{"ts": 1581098408, "adId": "ad-124", "userId": "user-001"}}
{"type":"click", "content":{"ts": 1581098412, "adId": "ad-123", "userId": "user-001"}}
{"type":"click", "content":{"ts": 1581098416, "adId": "ad-123", "userId": "user-001"}}
{"type":"click", "content":{"ts": 1581098420, "adId": "ad-123", "userId": "user-001"}}
{"type":"click", "content":{"ts": 1581098424, "adId": "ad-128", "userId": "user-001"}}

# Run the streaming application and observe the output

# This impressions info should have no impact (since the ts is smaller than the watermark)
# Watermark = 1581098416 - 5 seconds = 1581098411
{"ts": 1581098405, "adId": "ad-123"}

# This should match some of the clicks (since the ts is more than the watermark)
{"ts": 1581098412, "adId": "ad-123"}
'''
if __name__ == "__main__":
    ad = [{"type": "ad", "content": {"ts": 1581098400, "adId": "ad-123"}},
          {"type": "ad", "content": {"ts": 1581098403, "adId": "ad-124"}},
          {"type": "ad", "content": {"ts": 1581098406, "adId": "ad-125"}},
          {"type": "ad", "content": {"ts": 1581098409, "adId": "ad-126"}},
          {"type": "ad", "content": {"ts": 1581098412, "adId": "ad-127"}},
          {"type": "ad", "content": {"ts": 1581098416, "adId": "ad-128"}}]
    
