// Add this method to the LIPP class in ./lipp/src/core/lipp.h
// (Just before the end of the public section, around line 220)

void bulk_insert(const V* vs, int num_keys) {
    // If nothing to insert, return immediately
    if (num_keys == 0) return;
    
    // If the tree is empty, use bulk_load instead
    if (root->size == 0) {
        bulk_load(vs, num_keys);
        return;
    }
    
    // For small number of insertions, use regular insert
    if (num_keys < 100) {
        for (int i = 0; i < num_keys; i++) {
            insert(vs[i].first, vs[i].second);
        }
        return;
    }
    
    // For large number of insertions, we'll use a hybrid approach
    // Get the current data into an array
    T* keys = new T[root->size + num_keys];
    P* values = new P[root->size + num_keys];
    
    // Scan the current tree into the arrays
    scan_and_destory_tree(root, keys, values, false);
    
    // Merge the new data into the arrays
    int insert_pos = root->size;
    for (int i = 0; i < num_keys; i++) {
        keys[insert_pos] = vs[i].first;
        values[insert_pos] = vs[i].second;
        insert_pos++;
    }
    
    // Sort the merged array
    // Use a helper vector for sorting by key while maintaining value association
    std::vector<std::pair<T, int>> sort_helper(root->size + num_keys);
    for (int i = 0; i < root->size + num_keys; i++) {
        sort_helper[i] = std::make_pair(keys[i], i);
    }
    std::sort(sort_helper.begin(), sort_helper.end());
    
    // Reorder the arrays and remove duplicates (keeping the most recent)
    T* new_keys = new T[root->size + num_keys];
    P* new_values = new P[root->size + num_keys];
    int new_size = 0;
    
    for (int i = 0; i < root->size + num_keys; i++) {
        // Skip duplicates (keep the most recent which will be later in the insertion order)
        if (i > 0 && sort_helper[i].first == sort_helper[i-1].first) continue;
        
        int idx = sort_helper[i].second;
        new_keys[new_size] = keys[idx];
        new_values[new_size] = values[idx];
        new_size++;
    }
    
    // Destroy the old tree and build a new one with the merged data
    destroy_tree(root);
    root = build_tree_bulk(new_keys, new_values, new_size);
    
    // Clean up
    delete[] keys;
    delete[] values;
    delete[] new_keys;
    delete[] new_values;
}