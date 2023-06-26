import time
from torch.utils.data import Dataset, DataLoader

import pandas as pd
import numpy as np
from datetime import datetime

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import torch
import torch.nn as nn
from torch.autograd import Variable
import torch.optim as optim
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error


class StockDataset(Dataset):
    def __init__(self, data, seq_len=100):
        self.data = data
        self.data = torch.from_numpy(data).float().view(-1)
        self.seq_len = seq_len

    def __len__(self):
        return len(self.data) - self.seq_len - 1

    def __getitem__(self, index):
        return self.data[index: index + self.seq_len], self.data[index + self.seq_len]


class Lstm_model(nn.Module):
    def __init__(self, input_dim, hidden_size, num_layers):
        super(Lstm_model, self).__init__()
        self.num_layers = num_layers
        self.input_size = input_dim
        self.hidden_size = hidden_size
        self.lstm = nn.LSTM(input_size=input_dim, hidden_size=hidden_size, num_layers=num_layers)
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x, hn, cn):
        out, (hn, cn) = self.lstm(x, (hn, cn))
        final_out = self.fc(out[-1])
        return final_out, hn, cn

    def predict(self, x):
        out, (hn, cn) = self.init()
        final_out = self.fc(out[-1])
        return final_out

    def init(self, batch_size, device):
        h0 = torch.zeros(self.num_layers, batch_size, self.hidden_size).to(device)
        c0 = torch.zeros(self.num_layers, batch_size, self.hidden_size).to(device)
        return h0, c0


def train(epoch, model, device, dataloader, batch_size, criterion, optimizer):
    hn, cn = model.init(batch_size, device)
    model.train()

    overall_loss = 0.0
    num_samples = 0

    for batch, (x, y) in enumerate(dataloader):
        x = x.to(device)
        y = y.to(device)
        out, hn, cn = model(x.reshape(100, batch_size, 1), hn, cn)
        loss = criterion(out.reshape(batch_size), y)

        overall_loss += loss.item()
        num_samples += batch_size

        hn = hn.detach()
        cn = cn.detach()
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if (batch + 1) % 100 == 0:
            print(f'Epoch [{epoch + 1}/{epoch + 1}], '
                  f'Batch [{batch + 1}/{len(dataloader)}], '
                  f'Loss: {loss.item():.4f}')
    overall_loss = overall_loss / len(dataloader)
    # print(f'Overall Training Error: {overall_loss:.4f}')
    return overall_loss


def val(epoch, model, device, dataloader, batch_size, criterion):
    hn, cn = model.init(batch_size, device)
    model.eval()
    overall_loss = 0.0
    num_samples = 0
    with torch.no_grad():
        for batch, (x, y) in enumerate(dataloader):
            x = x.to(device)
            y = y.to(device)
            out, hn, cn = model(x.reshape(100, batch_size, 1), hn, cn)
            loss = criterion(out.reshape(batch_size), y)

            overall_loss += loss.item()

            hn = hn.detach().zero_()
            cn = cn.detach().zero_()

    overall_loss /= len(dataloader)
    # print(f"Overall Test Error: {overall_loss:.4f}")
    return overall_loss


def main():
    dataset = pd.read_csv(
        '/Users/munkhdelger/PycharmProjects/BDT_traffic/data/ATVS_timeseries.csv')  # Replace with the path to your dataset file
    # Select relevant columns as input features
    selected_columns = ['Yr', 'M', 'D', 'HH', 'MM', 'Vol']  # Add the desired numerical variables here
    input_data = dataset[selected_columns].values  # integer encode direction

    # ensure all data is float
    values = input_data.astype('float32')
    # normalize features
    scaler = MinMaxScaler(feature_range=(0, 1))
    input_scaled = scaler.fit_transform(values)

    scalar = MinMaxScaler(feature_range=(0, 1))  # Normalize numerical variables using MinMaxScaler
    input_data = scalar.fit_transform(np.array(input_data).reshape(-1, 1))

    training_size = int(len(input_data) * 0.8)
    train_data, test_data = input_data[0:training_size, :], input_data[training_size:len(input_data), :1]

    train_dataset = StockDataset(train_data)
    test_dataset = StockDataset(test_data)
    batch_size = 256
    train_dataloader = DataLoader(train_dataset, batch_size, shuffle=True, drop_last=True)
    test_dataloader = DataLoader(test_dataset, batch_size, shuffle=False, drop_last=True)

    input_dim = 1
    hidden_size = 10
    num_layers = 3

    device = torch.device("mps")

    model = Lstm_model(input_dim, hidden_size, num_layers).to(device)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.00001)

    best_val_loss = float('inf')
    epochs = 10
    for epoch in range(epochs):
        start = time.time()
        train_loss = train(epoch, model, device, train_dataloader, batch_size, criterion, optimizer)
        val_loss = val(epoch, model, device, test_dataloader, batch_size, criterion)

        end = time.time()
        print(f'Duration : {end - start}')
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_model = model.state_dict()
            torch.save(best_model, 'best_model.pt')

        print(f'---Epoch {epoch} --- t_loss={train_loss:.7f}, v_loss={val_loss:.7f}, b_loss={best_val_loss:.7f}')


if __name__ == "__main__":
    main()





