import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 曾刘拴
 * @className StaticUpDownFlow
 * @description TODO
 * @date 2021/07/17
 **/

public class StaticUpDownFlow {
    public static class StaticMapper extends  Mapper<Object, Text, Text, FlowBean> {
        public void map(Object key, Text value, Mapper<Object, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");
            int length = values.length;
            Text phone = new Text(values[1]);
            long upflow = Long.valueOf(values[length - 3]).longValue();
            long downflow = Long.valueOf(values[length - 2]).longValue();
            FlowBean flowBean = new FlowBean(upflow, downflow);
            context.write(phone, flowBean);
            System.out.println(phone + "--"+flowBean.getUpFlow() + "--" + flowBean.getDownFlow());
        }
    }

    public static class StaticReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        public void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            System.out.println("Reducer key: " + key);
            long up = 0L;
            long down = 0L;
            for (FlowBean flowBean : values) {
                up += flowBean.getUpFlow();
                down += flowBean.getDownFlow();
            }
            FlowBean flowBean = new FlowBean(up, down);
            System.out.println("put:" + key + "\t" + flowBean);

            context.write(key, flowBean);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        JobConf conf = new JobConf(StaticUpDownFlow.class);
        Job job = Job.getInstance(conf, "Stat");
        job.setNumReduceTasks(1);
        job.setJarByClass(StaticUpDownFlow.class);
        job.setMapperClass(StaticMapper.class);
        job.setCombinerClass(StaticReducer.class);
        job.setReducerClass(StaticReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

//        FileInputFormat.addInputPath(conf, new Path(args[0]));
//        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        FileInputFormat.addInputPath(conf, new Path("C:\\Users\\zengliushuan\\Desktop\\HTTP_20130313143750.dat"));
        FileOutputFormat.setOutputPath(conf, new Path("C:\\Users\\zengliushuan\\Desktop\\result"));
        JobClient.runJob(conf);
    }

}
