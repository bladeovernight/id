filename ='2\\text_2_var_38'
with open(filename) as file:
    lines = file.readlines()

sum_lines=list()

for line in lines:
    # print(line.strip())
    nums = line.split(",")
    #print (nums)
    sum_line = 0
    for num in nums:
        sum_line += int (num)
    
    sum_lines.append(sum_line)

    

with open('2\\r_text_2.txt','w') as result:
    for value in sum_lines:
        result.write(str(value) + "\n")
