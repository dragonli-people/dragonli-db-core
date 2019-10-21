# db core for db-service,db operate api for nodejs!
1 本项目有两种常见使用姿势：  
    # a 作为依赖引入，稍作包装成为一个微服务，供其它application(主要是nodejs等语言）使用   
    # b 作为java项目引入，初始化DbCore后，直接由java代码调用  
1 一些通用化配置，可在配置中心配置service.general.db-config节点指定内容。（如果没有特别的需求，建议沿用默认值） 
1 您可以通过--REDIS_KEY覆盖redis 2级缓存在redis中的路径前缀。当此值为空，则redis 2级缓存不开启。（如果没有特别的需求，并不建议开启此值）  
1 您可以通过--DEFAULT_DB_NAME覆盖默认数据库的值。不过此值已基本弃用，建议置空  
1 您可以通过--AUTO_UPDATE_TALBE_NAME覆盖"自动更新表"的表格。  
    ## 注：出于优化考虑，程序会缓存解析一个数据库所有表的结构。当增、减表或修改表结构时，程序是无法当表结构更改时，服务并不能捕捉到。解决方案是于程序启动前在数据库中创建"自动更新表"，内含一个int字段、仅1行。更改其值，会令程序重新解析所有表的结构  
1 您可以通过 --PRIMARY_KEY 覆盖默认主键的值。许多方法（如get方法）为了使调用方无需每次累赘地传入主键字段名，因此定义了此键值。  
    * 注：这也意味着所有表的主键都需要是这个名字，且自动递增。这是本库目前尚待改进之处  
1 您可以通过 --VERSION_KEY 覆盖版本字段的值。每次更新version字段都将递增  
    * 注：本功能暂未实现  
1 您可以通过 --TABLE_NAME_TAG 覆盖表tag的值。有些批量操作会对多个表的对象进行读写。因此，一个json对象传入时如果附加了这个tag字段，就相当于指定了表名  
    * 例：TABLE_NAME_TAG如果是__TABLE_NAME，则{"id":1,"title":"abc","content":"Article content...","__TABLE_NAME":"article"}，在批量保存接口中，将保存article表id为1的行  
