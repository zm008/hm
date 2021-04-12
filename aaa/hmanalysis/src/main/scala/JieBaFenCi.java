import com.huaban.analysis.jieba.JiebaSegmenter;

import java.util.List;

public class JieBaFenCi {


    public static List<String> getJieBaFenci(String name) {

        JiebaSegmenter segmenter = new JiebaSegmenter();

        return segmenter.sentenceProcess(name);



    }

    public static void main(String[] args) {
        System.out.println(getJieBaFenci("我们是中国人"));
    }
}
