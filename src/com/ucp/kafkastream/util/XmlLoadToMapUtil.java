package com.ucp.kafkastream.util;

import com.ucp.kafkastream.vo.XmlBean;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XmlLoadToMapUtil {
    private static Logger LOG = Logger.getLogger( XmlLoadToMapUtil.class );
    private static String xmlConfigPath = System.getProperty( "user.dir" ) + File.separator + "config" + File.separator + "book.xml";
    private XmlBean xmlBean = null;
    private static XmlLoadToMapUtil _instance = null;
    private static Map <String, XmlBean> xmlMap = new HashMap <String, XmlBean>();

    private XmlLoadToMapUtil() {
        parseXml( xmlConfigPath );
    }

    public static XmlLoadToMapUtil getInstance() {
        if (_instance == null) {
            synchronized (XmlLoadToMapUtil.class) {
                if (_instance == null) {
                    _instance = new XmlLoadToMapUtil();
                }
            }
        }
        return _instance;
    }

    public Map <String, XmlBean> getXmlMap() {
        return xmlMap;
    }

    public Map <String, XmlBean> parseXml(String loadfile) {
        ArrayList <String> arrayList = new ArrayList <>();
        FileInputStream fis = null;
        SAXReader reader = new SAXReader();
        Document document = null;
        try {
            fis = new FileInputStream( loadfile );
            document = reader.read( fis );
            //获取根节点
            Element roots = document.getRootElement();
            System.out.println( "roots:" + roots );
            //获取根节点下的所有子节点
//            List<Attribute> attributes = roots.attributes() ;
            List <Node> nodes = document.selectNodes( "/json/service" );
            //循环所有子元素
            if (nodes != null && nodes.size() > 0) {
                for (Node node : nodes) {
                    XmlBean xmlBean = new XmlBean();
                    String serviceId = node.getText().trim();
                    String times = node.selectSingleNode( "@times" ).getText().trim();
                    xmlBean.setTimes(times == null ? "" : times );
                    String flag = node.selectSingleNode( "@flag" ).getText().trim();
                    xmlBean.setFlag( flag == null ? "" : flag );
                    String cleanTime = node.selectSingleNode("@cleantime").getText().trim() ;
                    xmlBean.setCleanTime(cleanTime == null ? "" : cleanTime);
                    System.out.println( "serviceid=" + serviceId + ",name=" + times + ",flag=" + flag );
                    xmlMap.put( serviceId, xmlBean );
                }
            }
            return xmlMap;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
