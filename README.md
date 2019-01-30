# Storm-Jms-WordCount

在[ActiveMQ管理页面](http://localhost:8161/admin/queues.jsp) : <http://localhost:8161/admin/queues.jsp>，
发送Meesage到wordCount.queue.  
*Message后加上 **EOF***  (`WordCountBolt`中有用到)

---

词频统计结果会通过`sendBolt`推给ActiveMQ.

