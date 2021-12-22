package simpledb.storage;

import java.nio.file.DirectoryNotEmptyException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zwen
 * @Description
 * @create 2021-12-01 8:27 下午
 */
public class LRUReplacer {

    private static class dNode {
        int value;
        dNode next;
        dNode prev;
        dNode(int value) {
            this.value = value;
        }
    }

    private int capacity;
    private Map<Integer, dNode> map = new HashMap<>();
    private dNode sentinel;

    public LRUReplacer(int numPages) {
        sentinel = new dNode(-1);
        sentinel.next = sentinel;
        sentinel.prev = sentinel;
        capacity = numPages;
    }

//    public int Victim() {
//
//    }
//
//    void LRUReplacer::addToHead(const std::shared_ptr<Node> &node) {
//        node->prev = sentinel;
//        node->next = sentinel->next;
//        sentinel->next->prev = node;
//        sentinel->next = node;
//    }
//
//    void LRUReplacer::removeNode(const std::shared_ptr<Node> &node) {
//        node->prev->next = node->next;
//        node->next->prev = node->prev;
//    }
//
//    void LRUReplacer::moveToHead(const std::shared_ptr<Node> &node) {
//        removeNode(node);
//        addToHead(node);
//    }
//



}
