# spring-boot-starter-canal
## canal starter

### eg(annotation):

-------------------------
```
@CanalEventListener
public class MyEventListener {

    @InsertListenPoint
    public void onEvent(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        //do something...
    }

    @UpdateListenPoint
    public void onEvent1(CanalEntry.RowData rowData) {
        //do something...
    }

    @DeleteListenPoint
    public void onEvent3(CanalEntry.EventType eventType) {
        //do something...
    }

    @ListenPoint(destination = "example", schema = "canal-test", table = {"t_user", "test_table"}, eventType = CanalEntry.EventType.UPDATE)
    public void onEvent4(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        //do something...
    }
}
```

### eg(interface):
-------------------------------
```
@Component
public class MyEventListener2 implements CanalEventListener {
    @Override
    public void onEvent(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        //do something...
    }
}

@Component
public class MyEventListener2 implements DmlCanalEventListener {
    @Override
    public void onInsert(CanalEntry.RowData rowData) {
        //do something...
    }

    @Override
    public void onUpdate(CanalEntry.RowData rowData) {
        //do something...
    }

    @Override
    public void onDelete(CanalEntry.RowData rowData) {
        //do something...
    }
}

```
