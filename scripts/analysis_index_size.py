import pandas as pd


def result_analysis():
    tasks = ['fb'] #, 'osmc', 'books']
    indexs = ['BTree', 'DynamicPGM', 'LIPP', 'HybridPGMLIPP']
    # Create dictionaries to store index_size data for each index
    # lookuponly_index_size = {}
    # insertlookup_index_size = {}
    insertlookup_mix1_index_size = {}
    insertlookup_mix2_index_size = {}
    
    for index in indexs:
        # lookuponly_index_size[index] = {}
        # insertlookup_index_size[index] = {"lookup": {}, "insert": {}}
        insertlookup_mix1_index_size[index] = {}
        insertlookup_mix2_index_size[index] = {}
    
    for task in tasks:
        full_task_name = f"{task}_100M_public_uint64"
        # lookup_only_results = pd.read_csv(f"results/{full_task_name}_ops_2M_0.000000rq_0.500000nl_0.000000i_results_table.csv")
        # insert_lookup_results = pd.read_csv(f"results/{full_task_name}_ops_2M_0.000000rq_0.500000nl_0.500000i_0m_results_table.csv")
        insert_lookup_mix_1_results = pd.read_csv(f"results/{full_task_name}_ops_2M_0.000000rq_0.500000nl_0.100000i_0m_mix_results_table.csv")
        insert_lookup_mix_2_results = pd.read_csv(f"results/{full_task_name}_ops_2M_0.000000rq_0.500000nl_0.900000i_0m_mix_results_table.csv")
        
        for index in indexs:
            # find the row where lookup_only_result['index_name'] == index
            # try:
            #     lookup_only_result = lookup_only_results[lookup_only_results['index_name'] == index]
            #     # compute average index_size across lookup_only_result['index_size1'], lookup_only_result['index_size2'], lookup_only_result['index_size3'], then select the one with the highest index_size
            #     lookuponly_index_size[index][task] = lookup_only_result[['lookup_index_size_mops1', 'lookup_index_size_mops2', 'lookup_index_size_mops3']].mean(axis=1).max()
            # except:
            #     pass
            
            # # find the row where insert_lookup_result['index_name'] == index
            # try:
            #     insert_lookup_result = insert_lookup_results[insert_lookup_results['index_name'] == index]
            #     # compute average index_size across insert_lookup_result['index_size1'], insert_lookup_result['index_size2'], insert_lookup_result['index_size3'], then select the one with the highest index_size
            #     insertlookup_index_size[index]['lookup'][task] = insert_lookup_result[['lookup_index_size_mops1', 'lookup_index_size_mops2', 'lookup_index_size_mops3']].mean(axis=1).max()
            #     insertlookup_index_size[index]['insert'][task] = insert_lookup_result[['insert_index_size_mops1', 'insert_index_size_mops2', 'insert_index_size_mops3']].mean(axis=1).max()
            # except:
            #     pass
            
                
            # find the row where insert_lookup_mix_1_result['index_name'] == index
            try:
                insert_lookup_mix_1_result = insert_lookup_mix_1_results[insert_lookup_mix_1_results['index_name'] == index]
                # compute average index_size across insert_lookup_mix_1_result['index_size1'], insert_lookup_mix_1_result['index_size2'], insert_lookup_mix_1_result['index_size3'], then select the one with the highest index_size
                insertlookup_mix1_index_size[index][task] = insert_lookup_mix_1_result['index_size_bytes'].mean()
            except:
                pass
            
            
            # find the row where insert_lookup_mix_2_result['index_name'] == index
            try:
                insert_lookup_mix_2_result = insert_lookup_mix_2_results[insert_lookup_mix_2_results['index_name'] == index]
                # compute average index_size across insert_lookup_mix_2_result['index_size1'], insert_lookup_mix_2_result['index_size2'], insert_lookup_mix_2_result['index_size3'], then select the one with the highest index_size
                insertlookup_mix2_index_size[index][task] = insert_lookup_mix_2_result['index_size_bytes'].mean()
            except:
                pass
    # plot the figure of index_size, x axis is the index, y axis is the index_size
    # the figure should contain 4 subplots, each subplot corresponds to a workload, including lookup_only, insert_lookup, insert_lookup_mix1, insert_lookup_mix2
    # each subplot should contain 3 bars, each bar corresponds to a dataset (fb, osmc, books) if the index_size is not empty
    
    import matplotlib.pyplot as plt
    fig, axs = plt.subplots(2, 2, figsize=(10, 10))
    # Flatten axs for easier indexing
    axs = axs.flatten()
    
    # Define common plot parameters
    bar_width = 0.2
    index = range(len(indexs))
    colors = ['blue', 'green', 'red', 'orange']
    
    # # 1. Plot lookup-only index_size
    # ax = axs[0]
    # for i, task in enumerate(tasks):
    #     task_data = []
    #     for idx in indexs:
    #         task_data.append(lookuponly_index_size[idx].get(task, 0))
    #     ax.bar([x + i*bar_width for x in index], task_data, bar_width, label=task, color=colors[i])
        
    # ax.set_title('Lookup-only index_size')
    # ax.set_ylabel('index_size (Mops/s)')
    # ax.set_xticks([x + bar_width*1.5 for x in index])
    # ax.set_xticklabels(indexs)
    # ax.legend()
    
    # # 2. Plot insert-lookup index_size (separated)
    # ax = axs[1]
    # # First plot lookups
    # offset = 0
    # for i, task in enumerate(tasks):
    #     task_data = []
    #     for idx in indexs:
    #         task_data.append(insertlookup_index_size[idx]['lookup'].get(task, 0))
    #     ax.bar([x + offset for x in index], task_data, bar_width/2, 
    #            label=f'{task} (lookup)' if offset == 0 else "_nolegend_", 
    #            color=colors[i])
    #     offset += bar_width/2
    
    # # Then plot inserts
    # offset = bar_width*2
    # for i, task in enumerate(tasks):
    #     task_data = []
    #     for idx in indexs:
    #         task_data.append(insertlookup_index_size[idx]['insert'].get(task, 0))
    #     ax.bar([x + offset for x in index], task_data, bar_width/2, 
    #            label=f'{task} (insert)', color=colors[i], hatch='///')
    #     offset += bar_width/2
    
    # ax.set_title('Insert-Lookup index_size (50% insert ratio)')
    # ax.set_ylabel('index_size (Mops/s)')
    # ax.set_xticks([x + bar_width*1.5 for x in index])
    # ax.set_xticklabels(indexs)
    # ax.legend()
    
    # 3. Plot mixed workload with 10% inserts
    ax = axs[2]
    for i, task in enumerate(tasks):
        task_data = []
        for idx in indexs:
            task_data.append(insertlookup_mix1_index_size[idx].get(task, 0))
        ax.bar([x + i*bar_width for x in index], task_data, bar_width, label=task, color=colors[i])
        
    ax.set_title('Mixed Workload (10% insert ratio)')
    ax.set_ylabel('Index Size (Bytes)')
    ax.set_xticks([x + bar_width*1.5 for x in index])
    ax.set_xticklabels(indexs)
    ax.legend()
    
    # 4. Plot mixed workload with 90% inserts
    ax = axs[3]
    for i, task in enumerate(tasks):
        task_data = []
        for idx in indexs:
            task_data.append(insertlookup_mix2_index_size[idx].get(task, 0))
        ax.bar([x + i*bar_width for x in index], task_data, bar_width, label=task, color=colors[i])
        
    ax.set_title('Mixed Workload (90% insert ratio)')
    ax.set_ylabel('Index Size (Bytes)')
    ax.set_xticks([x + bar_width*1.5 for x in index])
    ax.set_xticklabels(indexs)
    ax.legend()
    
    # Add overall title and adjust layout
    fig.suptitle('Benchmark Results Across Different Workloads', fontsize=16)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    
    # Save the figure
    plt.savefig('benchmark_results_index_size.png', dpi=300)
    plt.show()
    
    # Save data to CSV files for further analysis
    import os
    os.makedirs('analysis_results', exist_ok=True)
    
    # pd.DataFrame(lookuponly_index_size).to_csv('analysis_results/lookuponly_index_size.csv')
    
    # lookup_df = pd.DataFrame({idx: data['lookup'] for idx, data in insertlookup_index_size.items()})
    # insert_df = pd.DataFrame({idx: data['insert'] for idx, data in insertlookup_index_size.items()})
    # lookup_df.to_csv('analysis_results/insertlookup_lookup_index_size.csv')
    # insert_df.to_csv('analysis_results/insertlookup_insert_index_size.csv')
    
    pd.DataFrame(insertlookup_mix1_index_size).to_csv('analysis_results/insertlookup_mix1_index_size.csv')
    pd.DataFrame(insertlookup_mix2_index_size).to_csv('analysis_results/insertlookup_mix2_index_size.csv')

if __name__ == "__main__":
    result_analysis()
