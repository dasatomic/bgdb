using Newtonsoft.Json;

namespace VideoProcessing
{
    [JsonObject]
    public class FfProbeOutputSerializer
    {
        [JsonObject]
        public class StreamSerializer
        {
            [JsonProperty("index")]
            public int Index;

            [JsonProperty("codec_name")]
            public string CodeName;

            [JsonProperty("codec_long_name")]
            public string CodecLongName;

            [JsonProperty("codec_type")]
            public string CodecType;

            [JsonProperty("width")]
            public int Width;

            [JsonProperty("height")]
            public int Height;

            [JsonProperty("has_b_frames")]
            public int HasBFrames;

            [JsonProperty("channels")]
            public int? Channels;

            [JsonProperty("sample_rate")]
            public int SampleRate;
        }

        [JsonObject]
        public class FormatSerializer
        {
            [JsonProperty("nb_streams")]
            public int NbStreams;

            [JsonProperty("nb_programs")]
            public int NbPrograms;

            [JsonProperty("duration")]
            public double DurationInSeconds;

            [JsonProperty("format_name")]
            public string FormatName;

            [JsonProperty("bit_rate")]
            public int BitRate;
        }

        [JsonProperty("streams")]
        public StreamSerializer[] Streams;

        [JsonProperty("format")]
        public FormatSerializer Format;
    }
}
